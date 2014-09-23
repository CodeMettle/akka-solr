/*
 * RequestHandler.scala
 *
 * Updated: Sep 22, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr
package client

import java.io.InputStream

import org.apache.solr.client.solrj.ResponseParser
import org.apache.solr.client.solrj.impl.{BinaryResponseParser, XMLResponseParser}
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.params.CommonParams
import org.apache.solr.common.util.NamedList
import spray.can.Http
import spray.http._

import com.codemettle.akkasolr.Solr.SolrOperation
import com.codemettle.akkasolr.client.RequestHandler.{Parsed, RespParserRetval, TimedOut}
import com.codemettle.akkasolr.util.ActorInputStream

import akka.actor._
import akka.pattern._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * @author steven
 *
 */
object RequestHandler {
    def props(baseUri: Uri, host: ActorRef, replyTo: ActorRef, request: SolrOperation, timeout: FiniteDuration) = {
        Props[RequestHandler](new RequestHandler(baseUri, host, replyTo, request, timeout))
    }

    private type RespParserRetval = Either[String, (ResponseParser, HttpCharset)]

    private case object TimedOut
    private case class Parsed(result: NamedList[AnyRef])
}

class RequestHandler(baseUri: Uri, host: ActorRef, replyTo: ActorRef, request: SolrOperation, timeout: FiniteDuration)
    extends Actor with ActorLogging {

    private val timer = {
        import context.dispatcher
        actorSystem.scheduler.scheduleOnce(timeout, self, TimedOut)
    }

    private var inputStream: ActorInputStream = _

    override def preStart() = {
        super.preStart()

        host ! createHttpRequest
    }

    override def postStop() = {
        super.postStop()

        timer.cancel()
    }

    private def sendError(err: Throwable) = {
        replyTo ! Status.Failure(err)
        context stop self
    }

    private def createHttpRequest = request match {
        case Solr.Ping(action, _) ⇒
            val p = new /*Binary*/XMLResponseParser

            val baseQuery = Uri.Query(
                CommonParams.VERSION → p.getVersion,
                CommonParams.WT      → p.getWriterType
            )

            val query = action.fold(baseQuery) {
                case Solr.Ping.Enable  ⇒ (CommonParams.ACTION → CommonParams.ENABLE)  +: baseQuery
                case Solr.Ping.Disable ⇒ (CommonParams.ACTION → CommonParams.DISABLE) +: baseQuery
            }

            //HttpRequest(HttpMethods.GET, baseUri.pingUri withQuery query)
            HttpRequest(HttpMethods.POST, baseUri.pingUri,
                entity = HttpEntity(ContentType(MediaTypes.`application/x-www-form-urlencoded`, HttpCharsets.`UTF-8`),
                    query.toString()))

        case Solr.Select(params, _) ⇒
            val p = new /*XML*/BinaryResponseParser

            val baseQuery = Uri.Query(
                CommonParams.VERSION → p.getVersion,
                CommonParams.WT → p.getWriterType
            )

            val query = (CommonParams.VERSION → p.getVersion) +: ((CommonParams.WT → p.getWriterType) +: params.toQuery)

            HttpRequest(HttpMethods.GET, baseUri.selectUri withQuery query)
//            HttpRequest(HttpMethods.POST, baseUri.selectUri,
//                entity = HttpEntity(ContentType(MediaTypes.`application/x-www-form-urlencoded`, HttpCharsets.`UTF-8`),
//                    query.toString()))
    }

    private def getContentType(implicit resp: HttpResponse) = {
        (resp.headers collect {
            case HttpHeaders.`Content-Type`(ct) ⇒ ct
        }).headOption
    }

    private def createResponseParser(implicit resp: HttpResponse) = {
        getContentType.fold[RespParserRetval](Left("No Content-Type header found")) (ct ⇒ {
            ct.mediaType match {
                case MediaTypes.`application/xml` ⇒ Right(new XMLResponseParser → ct.charset)

                case MediaTypes.`application/octet-stream` ⇒ Right(new BinaryResponseParser → ct.charset)

                case _ ⇒ Left(s"Unsupported response content type: ${ct.mediaType}")
            }
        })
    }

    private def processResponse(chunkStart: Boolean)(implicit resp: HttpResponse): Unit = {
        def doCreateResponseParser(is: InputStream) = createResponseParser match {
            case Left(err) ⇒ sendError(Solr.InvalidResponse(err))

            case Right((parser, charset)) ⇒
                implicit val dispatcher = Solr.Client.responseParserDispatcher

                Future(parser.processResponse(is, charset.value)) map Parsed pipeTo self
        }

        resp.status match {
            case StatusCodes.RequestEntityTooLarge ⇒
                sendError(Solr.ServerError(resp.status, "Try sending large queries as POST instead of GET"))

            case _ ⇒ // we can add more special cases as they arise
                {
                    if (chunkStart) {
                        inputStream = new ActorInputStream
                        Right(inputStream)
                    } else resp.entity.data match {
                        case HttpData.Bytes(bytes) ⇒ Right(bytes.iterator.asInputStream)
                        case _ ⇒ Left(Solr.InvalidResponse(s"Don't know how to handle entity type ${resp.entity.data.getClass.getSimpleName}"))
                    }
                } match {
                    case Left(err) ⇒ sendError(err)
                    case Right(is) ⇒ doCreateResponseParser(is)
                }
        }
    }

    def receive = {
        case TimedOut ⇒ sendError(Solr.RequestTimedOut(request.options.requestTimeout))

        case resp: HttpResponse ⇒
            log.debug("got non-chunked response: {}", resp)
            processResponse(chunkStart = false)(resp)

        case ChunkedResponseStart(resp) ⇒
            log.debug("response started: {}", resp)
            processResponse(chunkStart = true)(resp)

        case MessageChunk(data, _) ⇒ data match {
            case HttpData.Bytes(bytes) ⇒ inputStream enqueueBytes bytes

            case _ ⇒ sendError(
                Solr.InvalidResponse(s"Don't know how to handle message chunk type ${data.getClass.getSimpleName}"))
        }

        case _: ChunkedMessageEnd ⇒ inputStream.streamFinished()

        case Status.Failure(e: Http.ConnectionException) ⇒ sendError(e)

        case Status.Failure(t) ⇒ sendError(Solr.ParseError(t))

        case Parsed(result) ⇒
            replyTo ! new QueryResponse(result, null)
            context stop self

        case m ⇒
            log.warning("Unhandled message: {}", m)
    }
}
