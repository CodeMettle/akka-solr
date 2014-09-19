package com.github.akkasolr.client

import com.github.akkasolr.Solr.SolrOperation
import com.github.akkasolr.client.ConnectingStasher._
import spray.can.Http

import akka.actor._
import scala.concurrent.duration._

/**
 * @author steven
 *
 */
object ConnectingStasher {
    def props = {
        Props[ConnectingStasher]
    }

    private[client] case class WaitingRequest(sender: ActorRef, req: SolrOperation, timeout: FiniteDuration,
                                              recvdTime: Long = System.nanoTime())

    private[client] case object FlushWaitingRequests

    private[client] case class StashedRequest(sender: ActorRef, req: SolrOperation, remainingTimeout: FiniteDuration)

    private[client] case class ErrorOutAllWaiting(err: Throwable)

    private case class TimedOut(req: WaitingRequest)
}

class ConnectingStasher extends Actor {
    import context.dispatcher

    private var stashed = Map.empty[WaitingRequest, Cancellable]

    def receive = {
        case wr: WaitingRequest ⇒
            val t = context.system.scheduler.scheduleOnce(wr.timeout, self, TimedOut(wr))
            stashed += (wr → t)

        case TimedOut(wr @ WaitingRequest(replyTo, req, _, _)) ⇒
            replyTo ! Status.Failure(new Http.ConnectionException(s"Connection not established within ${req.timeout}"))
            stashed -= wr

        case ErrorOutAllWaiting(t) ⇒
            val msg = Status.Failure(t)
            stashed.values foreach (_.cancel())
            stashed.keys foreach (_.sender ! msg)
            stashed = Map.empty

        case FlushWaitingRequests ⇒
            val now = System.nanoTime()
            stashed.values foreach (_.cancel())
            stashed.keys foreach {
                case WaitingRequest(act, req, timeout, recvd) ⇒
                    sender() ! StashedRequest(act, req, timeout - (now - recvd).nanos)
            }
            stashed = Map.empty
    }
}
