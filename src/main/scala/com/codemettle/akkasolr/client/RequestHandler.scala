/*
 * RequestHandler.scala
 *
 * Updated: Oct 23, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr
package client

import java.{lang => jl}

import org.apache.solr.client.solrj.impl.{BinaryResponseParser, StreamingBinaryResponseParser, XMLResponseParser}
import org.apache.solr.client.solrj.{ResponseParser, StreamingResponseCallback}
import org.apache.solr.common.SolrDocument
import org.apache.solr.common.params.{CommonParams, UpdateParams}
import org.apache.solr.common.util.NamedList

import com.codemettle.akkasolr.Solr.{RequestMethods, SolrOperation, SolrResponseTypes}
import com.codemettle.akkasolr.client.ClientConnection.RequestQueue
import com.codemettle.akkasolr.client.RequestHandler.{BodyReadComplete, Parsed, TimedOut}
import com.codemettle.akkasolr.solrtypes.{AkkaSolrDocument, SolrQueryResponse, SolrResultInfo}
import com.codemettle.akkasolr.util.{ActorInputStream, Util}

import akka.actor._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.pattern.pipe
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * @author steven
 *
 */
object RequestHandler {
    def props(baseUri: Uri, username: Option[String], password: Option[String], requestQueue: RequestQueue,
              replyTo: ActorRef, request: SolrOperation, timeout: FiniteDuration)(implicit mat: Materializer) =
        Props(new RequestHandler(baseUri, username, password, requestQueue, replyTo, request, timeout))

    private type RespParserRetval = Either[String, (ResponseParser, HttpCharset)]

    private case object TimedOut
    private case object BodyReadComplete
    private case class Parsed(result: NamedList[AnyRef])
}

class RequestHandler(baseUri: Uri, username: Option[String], password: Option[String], requestQueue: RequestQueue,
                     replyTo: ActorRef, request: SolrOperation, timeout: FiniteDuration)(implicit mat: Materializer)
    extends Actor with ActorLogging {

    import context.dispatcher

    class StreamCallback extends StreamingResponseCallback {
        override def streamSolrDocument(doc: SolrDocument): Unit = {
            sendMessage(AkkaSolrDocument(doc))
        }

        override def streamDocListInfo(numFound: Long, start: Long, maxScore: jl.Float): Unit = {
            sendMessage(SolrResultInfo(numFound, start, maxScore))
        }
    }

    private val timer = actorSystem.scheduler.scheduleOnce(timeout, self, TimedOut)

    private var shutdownTimer = Option.empty[Cancellable]

    private var timedOut = false
    private var receivedResponse = false
    private var chunkingDone = false
    private var sentResponse = false

    private var updateOptions = Option.empty[Solr.UpdateOptions]

    override def preStart(): Unit = {
        super.preStart()

        def authenticate(req: HttpRequest) = {
            def credsOpt = for (u <- username; p <- password) yield BasicHttpCredentials(u, p)
            def headerOpt = credsOpt map (Authorization(_))
            headerOpt.fold(req)(head => req.mapHeaders(head +: _))
        }

        checkCreateHttpRequest match {
            case Left(err) => sendError(None, Solr.InvalidRequest(err))
            case Right(req) => requestQueue.queueRequest(authenticate(req)) pipeTo self
        }
    }

    override def postStop(): Unit = {
        super.postStop()

        timer.cancel()
        shutdownTimer foreach (_.cancel())
    }

    private def sendMessage(m: Any): Unit = {
        replyTo.tell(m, context.parent)
    }

    private def startShutdown(): Unit = {
        if (chunkingDone)
            self ! PoisonPill
        else {
            import context.dispatcher

            import scala.concurrent.duration._

            shutdownTimer = Some(actorSystem.scheduler.scheduleOnce(1.second, self, PoisonPill))
        }
    }

    private def checkShutdown(): Unit = {
        if (chunkingDone && sentResponse)
            self ! PoisonPill
    }

    private def finishedParsing(result: NamedList[AnyRef]): Unit = {
        val resp = SolrQueryResponse(request, result)
        sendMessage {
            updateOptions.fold[Any](resp) { implicit opts =>
              resp.toFailMessage match {
                  case Left(err) => Status.Failure(err)
                  case Right(res) => res
              }
            }
        }
        sentResponse = true
        startShutdown()
    }

    private def chunkingFinished(): Unit = {
        chunkingDone = true
        checkShutdown()
    }

    private def sendError(respOpt: Option[HttpResponse], err: Throwable): Unit = {
        respOpt.foreach(_.discardEntityBytes())
        sendMessage(Status.Failure(err))
        sentResponse = true
        self ! PoisonPill
    }

    private def createQueryRequest = {
        val parser = request.options.responseType match {
            case SolrResponseTypes.Binary => new BinaryResponseParser
            case SolrResponseTypes.XML => new XMLResponseParser
            case SolrResponseTypes.Streaming => new StreamingBinaryResponseParser(null)
        }

        val baseQuery = Uri.Query(
            CommonParams.VERSION -> parser.getVersion,
            CommonParams.WT -> parser.getWriterType
        )

        val (uri, addlQuery) = request match {
            case Solr.Ping(action, _) =>
                baseUri.pingUri -> action.fold(Uri.Query.Empty: Uri.Query) {
                    case Solr.Ping.Enable => Uri.Query(CommonParams.ACTION -> CommonParams.ENABLE)
                    case Solr.Ping.Disable => Uri.Query(CommonParams.ACTION -> CommonParams.DISABLE)
                }

            case Solr.Select(params, _) => baseUri.selectUri -> params.toQuery

            case Solr.Commit(waitSearch, soft, _) =>
                baseUri.updateUri -> Uri.Query(
                    UpdateParams.COMMIT -> "true",
                    UpdateParams.SOFT_COMMIT -> soft.toString,
                    UpdateParams.WAIT_SEARCHER -> waitSearch.toString
                )

            case Solr.Optimize(waitSearch, maxSegs, _) =>
                baseUri.updateUri -> Uri.Query(
                    UpdateParams.OPTIMIZE -> "true",
                    UpdateParams.MAX_OPTIMIZE_SEGMENTS -> maxSegs.toString,
                    UpdateParams.WAIT_SEARCHER -> waitSearch.toString
                )

            case Solr.Rollback(_) =>
                baseUri.updateUri -> Uri.Query(UpdateParams.ROLLBACK -> "true")

            case _: Solr.Update => sys.error("Shouldn't have made it here")
        }

        val query = addlQuery.foldRight(baseQuery) {
            case ((k, v), acc) => (k -> v) +: acc
        }

        request.options.method match {
            case RequestMethods.GET =>
                HttpRequest(HttpMethods.GET, uri withQuery query)

            case RequestMethods.POST =>
                HttpRequest(HttpMethods.POST, uri, entity = HttpEntity(
                    ContentType(MediaTypes.`application/x-www-form-urlencoded`),
                    query.toString()))
        }
    }

    private def createHttpRequest = {
        request match {
            case su@Solr.Update(_, _, _, opts, _) =>
                updateOptions = Some(opts)

                val request = su.basicUpdateRequest

                val query = {
                    if (opts.commit)
                        Uri.Query(UpdateParams.COMMIT -> "true")
                    else if (opts.commitWithin.isDefined)
                        Uri.Query(UpdateParams.COMMIT_WITHIN -> opts.commitWithin.get.toMillis.toString)
                    else
                        Uri.Query.Empty
                }

                HttpRequest(HttpMethods.POST, baseUri.updateUri withQuery query,
                    entity = HttpEntity(ContentType(MediaTypes.`application/xml`, HttpCharsets.`UTF-8`),
                        Util updateRequestToByteString request))

            case _ => createQueryRequest
        }
    }

    private def checkCreateHttpRequest = {
        if (request.options.responseType == SolrResponseTypes.Streaming) {
            request match {
                case Solr.Select(_, _) => Right(createHttpRequest)
                case _ => Left("Streaming responses can only be requested for Select operations")
            }
        } else Right(createHttpRequest)
    }

    private def createResponseParser(implicit resp: HttpResponse) = {
        val mediaType = resp.entity.contentType.mediaType
        val charset = resp.entity.contentType.charsetOption.getOrElse(HttpCharsets.`UTF-8`)

        mediaType match {
            case MediaTypes.`application/xml` => Right(new XMLResponseParser -> charset)

            case MediaTypes.`application/octet-stream` if request.options.responseType ==
                SolrResponseTypes.Streaming =>
                Right(new StreamingBinaryResponseParser(new StreamCallback) -> charset)

            case MediaTypes.`application/octet-stream` => Right(new BinaryResponseParser -> charset)

            case _ => Left(s"Unsupported response content type: $mediaType")
        }
    }

    private def processResponse(implicit resp: HttpResponse): Unit = {
        def doCreateResponseParser(dataBytes: Source[ByteString, Any]) = createResponseParser match {
            case Left(err) => sendError(Some(resp), Solr.InvalidResponse(err))

            case Right((parser, charset)) =>
                val is = new ActorInputStream()

            {
                import context.dispatcher

                dataBytes.runWith(Sink.foreach(is.enqueueBytes)) onComplete {
                    case Success(_) =>
                    is.streamFinished()
                    self ! BodyReadComplete

                    case Failure(t) => self ! Status.Failure(t)
                }
            }

            {
                implicit val dispatcher: ExecutionContext = Solr.Client.responseParserDispatcher
                Future(parser.processResponse(is, charset.value)) map Parsed recover {
                    case t => Status.Failure(Solr.ParseError(t))
                } pipeTo self
            }
        }

        resp.status match {
                /*  server sends a useful response for a 400...others might too, TODO test other errors
            case StatusCodes.BadRequest =>
                sendError(Solr.ServerError(resp.status, s"Is the query malformed? Does it have more than ${
                    Solr.Client.maxBooleanClauses
                } boolean clauses?"))
                */

            case StatusCodes.Forbidden =>
                sendError(Some(resp), Solr.ServerError(resp.status, "Authentication is currently unsupported"))

            case StatusCodes.ServiceUnavailable =>
                sendError(Some(resp), Solr.ServerError(resp.status, "Solr may be shutting down or overloaded"))

            case StatusCodes.InternalServerError =>
                sendError(Some(resp), Solr.ServerError(resp.status, "Probably a transient error"))

            case StatusCodes.RequestEntityTooLarge =>
                sendError(Some(resp), Solr.ServerError(resp.status, "Try sending large queries as POST instead of GET"))

            case StatusCodes.NotFound =>
                sendError(Some(resp), Solr.ServerError(resp.status, s"Is '${baseUri.path}' the correct address to Solr?"))

            case _ =>
                // we can add more special cases as they arise
                doCreateResponseParser(resp.entity.dataBytes)
        }
    }

    override def receive: Receive = {
        case TimedOut if receivedResponse => sendError(None, Solr.RequestTimedOut(request.requestTimeout))
        case TimedOut => timedOut = true

        case resp: HttpResponse if timedOut => sendError(Some(resp), Solr.RequestTimedOut(request.requestTimeout))

        case resp: HttpResponse =>
            receivedResponse = true
            log.debug("response started: {}", resp)
            processResponse/*(chunkStart = false)*/(resp)

        case BodyReadComplete => chunkingFinished()

        case Status.Failure(t) => sendError(None, t)

        case Parsed(result) => finishedParsing(result)

        case m => log.warning("Unhandled message: {}", m)
    }
}
