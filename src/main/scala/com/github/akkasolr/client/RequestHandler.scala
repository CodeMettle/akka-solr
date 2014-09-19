package com.github.akkasolr
package client

import com.github.akkasolr.Solr.SolrOperation
import com.github.akkasolr.client.RequestHandler.{Parsed, RespParserRetval, TimedOut}
import com.github.akkasolr.util.ActorInputStream
import org.apache.solr.client.solrj.ResponseParser
import org.apache.solr.client.solrj.impl.{BinaryResponseParser, XMLResponseParser}
import org.apache.solr.common.params.CommonParams
import org.apache.solr.common.util.NamedList
import spray.http.Uri.Query
import spray.http._

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

    private var response = Option.empty[HttpResponse]
    private var inputStream: ActorInputStream = _
    private var responseParser: ResponseParser = _

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
            val p = new XMLResponseParser

            val baseQuery = Query(
                CommonParams.VERSION → p.getVersion,
                CommonParams.WT      → p.getWriterType
            )

            val query = action.fold(baseQuery) {
                case Solr.Ping.Enable  ⇒ (CommonParams.ACTION → CommonParams.ENABLE)  +: baseQuery
                case Solr.Ping.Disable ⇒ (CommonParams.ACTION → CommonParams.DISABLE) +: baseQuery
            }

            HttpRequest(HttpMethods.GET, baseUri.pingUri withQuery query)
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

    def receive = {
        case TimedOut ⇒ sendError(Solr.RequestTimedOut(request.timeout))

        case ChunkedResponseStart(resp) ⇒
            log.debug("response started: {}", resp)
            response = Some(resp)
            inputStream = new ActorInputStream
            createResponseParser(resp) match {
                case Left(err) ⇒ sendError(Solr.InvalidResponse(err))

                case Right((parser, charset)) ⇒
                    responseParser = parser

                    implicit val dispatcher = Solr.Client.responseParserDispatcher

                    Future(responseParser.processResponse(inputStream, charset.value)) map Parsed pipeTo self
            }

        case MessageChunk(data, _) ⇒ data match {
            case HttpData.Bytes(bytes) ⇒ inputStream enqueueBytes bytes

            case _ ⇒ sendError(
                Solr.InvalidResponse(s"Don't know how to handle message chunk type ${data.getClass.getSimpleName}"))
        }

        case _: ChunkedMessageEnd ⇒ inputStream.streamFinished()

        case Status.Failure(t) ⇒ sendError(Solr.ParseError(t))

        case Parsed(result) ⇒
            replyTo ! result
            context stop self

        case m ⇒
            log.warning("Unhandled message: {}", m)
    }
}
