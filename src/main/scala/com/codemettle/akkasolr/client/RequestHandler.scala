/*
 * RequestHandler.scala
 *
 * Updated: Oct 23, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr
package client

import java.io.InputStream
import java.{lang ⇒ jl}

import org.apache.solr.client.solrj.impl.{BinaryResponseParser, StreamingBinaryResponseParser, XMLResponseParser}
import org.apache.solr.client.solrj.{ResponseParser, StreamingResponseCallback}
import org.apache.solr.common.SolrDocument
import org.apache.solr.common.params.{CommonParams, UpdateParams}
import org.apache.solr.common.util.NamedList
import spray.http._

import com.codemettle.akkasolr.Solr.{RequestMethods, SolrOperation, SolrResponseTypes}
import com.codemettle.akkasolr.client.RequestHandler.{Parsed, RespParserRetval, TimedOut}
import com.codemettle.akkasolr.solrtypes.{AkkaSolrDocument, SolrQueryResponse, SolrResultInfo}
import com.codemettle.akkasolr.util.{ActorInputStream, Util}

import akka.actor._
import akka.pattern._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * @author steven
 *
 */
object RequestHandler {
    def props(baseUri: Uri, username: Option[String], password: Option[String], host: ActorRef, replyTo: ActorRef,
              request: SolrOperation, timeout: FiniteDuration) = {
        Props(new RequestHandler(baseUri, username, password, host, replyTo, request, timeout))
    }

    private type RespParserRetval = Either[String, (ResponseParser, HttpCharset)]

    private case object TimedOut
    private case class Parsed(result: NamedList[AnyRef])
}

class RequestHandler(baseUri: Uri, username: Option[String], password: Option[String], host: ActorRef,
                     replyTo: ActorRef, request: SolrOperation, timeout: FiniteDuration)
    extends Actor with ActorLogging {
    class StreamCallback extends StreamingResponseCallback {
        override def streamSolrDocument(doc: SolrDocument): Unit = {
            sendMessage(AkkaSolrDocument(doc))
        }

        override def streamDocListInfo(numFound: Long, start: Long, maxScore: jl.Float): Unit = {
            sendMessage(SolrResultInfo(numFound, start, maxScore))
        }
    }

    private val timer = {
        import context.dispatcher
        actorSystem.scheduler.scheduleOnce(timeout, self, TimedOut)
    }

    private var shutdownTimer = Option.empty[Cancellable]

    private var inputStream = Option.empty[ActorInputStream]

    private var chunkingResponse = false
    private var chunkingDone = false
    private var sentResponse = false

    override def preStart() = {
        super.preStart()

        def authenticate(req: HttpRequest) = {
            def credsOpt = for (u ← username; p ← password) yield BasicHttpCredentials(u, p)
            def headerOpt = credsOpt map (HttpHeaders.Authorization(_))
            headerOpt.fold(req)(head ⇒ req.mapHeaders(head :: _))
        }

        checkCreateHttpRequest match {
            case Left(err) ⇒ sendError(Solr.InvalidRequest(err))
            case Right(req) ⇒ host ! authenticate(req)
        }
    }

    override def postStop() = {
        super.postStop()

        timer.cancel()
        shutdownTimer foreach (_.cancel())
    }

    private def sendMessage(m: Any) = {
        replyTo.tell(m, context.parent)
    }

    private def startShutdown() = {
        if (!chunkingResponse)
            self ! PoisonPill
        else {
            if (chunkingDone)
                self ! PoisonPill
            else {
                import context.dispatcher

                import scala.concurrent.duration._

                shutdownTimer = Some(actorSystem.scheduler.scheduleOnce(1.second, self, PoisonPill))
            }
        }
    }

    private def checkShutdown() = {
        if (chunkingDone && sentResponse)
            self ! PoisonPill
    }

    private def finishedParsing(result: NamedList[AnyRef]) = {
        sendMessage(SolrQueryResponse(request, result))
        sentResponse = true
        startShutdown()
    }

    private def chunkingFinished() = {
        inputStream foreach (_.streamFinished())
        chunkingDone = true
        checkShutdown()
    }

    private def sendError(err: Throwable) = {
        sendMessage(Status.Failure(err))
        sentResponse = true
        self ! PoisonPill
    }

    private def createQueryRequest = {
        val parser = request.options.responseType match {
            case SolrResponseTypes.Binary ⇒ new BinaryResponseParser
            case SolrResponseTypes.XML ⇒ new XMLResponseParser
            case SolrResponseTypes.Streaming ⇒ new StreamingBinaryResponseParser(null)
        }

        val baseQuery = Uri.Query(
            CommonParams.VERSION → parser.getVersion,
            CommonParams.WT → parser.getWriterType
        )

        val (uri, addlQuery) = request match {
            case Solr.Ping(action, _) ⇒
                baseUri.pingUri → action.fold(Uri.Query.Empty: Uri.Query) {
                    case Solr.Ping.Enable ⇒ Uri.Query(CommonParams.ACTION → CommonParams.ENABLE)
                    case Solr.Ping.Disable ⇒ Uri.Query(CommonParams.ACTION → CommonParams.DISABLE)
                }

            case Solr.Select(params, _) ⇒ baseUri.selectUri → params.toQuery

            case Solr.Commit(waitSearch, soft, _) ⇒
                baseUri.updateUri → Uri.Query(
                    UpdateParams.COMMIT → "true",
                    UpdateParams.SOFT_COMMIT → soft.toString,
                    UpdateParams.WAIT_SEARCHER → waitSearch.toString
                )

            case Solr.Optimize(waitSearch, maxSegs, _) ⇒
                baseUri.updateUri → Uri.Query(
                    UpdateParams.OPTIMIZE → "true",
                    UpdateParams.MAX_OPTIMIZE_SEGMENTS → maxSegs.toString,
                    UpdateParams.WAIT_SEARCHER → waitSearch.toString
                )

            case Solr.Rollback(_) ⇒
                baseUri.updateUri → Uri.Query(UpdateParams.ROLLBACK → "true")

            case _: Solr.Update ⇒ sys.error("Shouldn't have made it here")
        }

        val query = (addlQuery :\ baseQuery) {
            case ((k, v), acc) ⇒ (k → v) +: acc
        }

        request.options.method match {
            case RequestMethods.GET ⇒
                HttpRequest(HttpMethods.GET, uri withQuery query)

            case RequestMethods.POST ⇒
                HttpRequest(HttpMethods.POST, uri, entity = HttpEntity(
                    ContentType(MediaTypes.`application/x-www-form-urlencoded`, HttpCharsets.`UTF-8`),
                    query.toString()))
        }
    }

    private def createHttpRequest = {
        request match {
            case su@Solr.Update(addDocs, deleteIds, deleteQueries, opts, _) ⇒
                val request = su.basicUpdateRequest

                val query = {
                    if (opts.commit)
                        Uri.Query(UpdateParams.COMMIT → "true")
                    else if (opts.commitWithin.isDefined)
                        Uri.Query(UpdateParams.COMMIT_WITHIN → opts.commitWithin.get.toMillis.toString)
                    else
                        Uri.Query.Empty
                }

                HttpRequest(HttpMethods.POST, baseUri.updateUri withQuery query,
                    entity = HttpEntity(ContentType(MediaTypes.`application/xml`, HttpCharsets.`UTF-8`),
                        Util updateRequestToByteString request))

            case _ ⇒ createQueryRequest
        }
    }

    private def checkCreateHttpRequest = {
        if (request.options.responseType == SolrResponseTypes.Streaming) {
            request match {
                case Solr.Select(_, _) ⇒ Right(createHttpRequest)
                case _ ⇒ Left("Streaming responses can only be requested for Select operations")
            }
        } else Right(createHttpRequest)
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

                case MediaTypes.`application/octet-stream` if request.options.responseType ==
                    SolrResponseTypes.Streaming ⇒
                    Right(new StreamingBinaryResponseParser(new StreamCallback) → ct.charset)

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

                Future(parser.processResponse(is, charset.value)) map Parsed recover
                    { case t ⇒ Solr.ParseError(t)} pipeTo self
        }

        resp.status match {
                /*  server sends a useful response for a 400...others might too, TODO test other errors
            case StatusCodes.BadRequest ⇒
                sendError(Solr.ServerError(resp.status, s"Is the query malformed? Does it have more than ${
                    Solr.Client.maxBooleanClauses
                } boolean clauses?"))
                */

            case StatusCodes.Forbidden ⇒
                sendError(Solr.ServerError(resp.status, "Authentication is currently unsupported"))

            case StatusCodes.ServiceUnavailable ⇒
                sendError(Solr.ServerError(resp.status, "Solr may be shutting down or overloaded"))

            case StatusCodes.InternalServerError ⇒
                sendError(Solr.ServerError(resp.status, "Probably a transient error"))

            case StatusCodes.RequestEntityTooLarge ⇒
                sendError(Solr.ServerError(resp.status, "Try sending large queries as POST instead of GET"))

            case StatusCodes.NotFound ⇒
                sendError(Solr.ServerError(resp.status, s"Is '${baseUri.path}' the correct address to Solr?"))

            case _ ⇒ // we can add more special cases as they arise
                {
                    if (chunkStart) {
                        inputStream = Some(new ActorInputStream)
                        Right(inputStream.get)
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
        case TimedOut ⇒ sendError(Solr.RequestTimedOut(request.requestTimeout))

        case resp: HttpResponse ⇒
            log.debug("got non-chunked response: {}", resp)
            processResponse(chunkStart = false)(resp)

        case ChunkedResponseStart(resp) ⇒
            log.debug("response started: {}", resp)
            chunkingResponse = true
            processResponse(chunkStart = true)(resp)

        case MessageChunk(data, _) ⇒ data match {
            case HttpData.Bytes(bytes) ⇒ inputStream foreach (_ enqueueBytes bytes)

            case _ ⇒ sendError(
                Solr.InvalidResponse(s"Don't know how to handle message chunk type ${data.getClass.getSimpleName}"))
        }

        case _: ChunkedMessageEnd ⇒ chunkingFinished()

        case Status.Failure(t) ⇒ sendError(t)

        case Parsed(result) ⇒ finishedParsing(result)

        case m ⇒ log.warning("Unhandled message: {}", m)
    }
}
