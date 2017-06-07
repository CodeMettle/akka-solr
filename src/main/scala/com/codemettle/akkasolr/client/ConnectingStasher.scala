/*
 * ConnectingStasher.scala
 *
 * Updated: Oct 16, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.client

import com.codemettle.akkasolr.Solr
import com.codemettle.akkasolr.client.ConnectingStasher._

import akka.actor._
import scala.concurrent.duration._

/**
 * @author steven
 *
 */
object ConnectingStasher {
    def props = Props(new ConnectingStasher)

    private[client] case class WaitingRequest(sender: ActorRef, req: Any, timeoutRemaining: FiniteDuration,
                                              originalTimeout: FiniteDuration, recvdTime: Long = System.nanoTime())

    private[client] case object FlushWaitingRequests

    private[client] case class StashedRequest(sender: ActorRef, req: Any, remainingTimeout: FiniteDuration,
                                              originalTimeout: FiniteDuration)

    private[client] case class ErrorOutAllWaiting(err: Throwable)

    private case class TimedOut(req: WaitingRequest)
}

class ConnectingStasher extends Actor {
    import context.dispatcher

    private var stashed = Map.empty[WaitingRequest, Cancellable]

    override def receive: Receive = {
        case wr: WaitingRequest ⇒
            val t = context.system.scheduler.scheduleOnce(wr.timeoutRemaining, self, TimedOut(wr))
            stashed += (wr → t)

        case TimedOut(wr @ WaitingRequest(replyTo, _, _, origTimeout, _)) ⇒
            val msg = Status.Failure(Solr.ConnectionException(s"Connection not established within $origTimeout"))
            replyTo.tell(msg, context.parent)
            stashed -= wr

        case ErrorOutAllWaiting(t) ⇒
            val msg = Status.Failure(t)
            stashed.values foreach (_.cancel())
            stashed.keys foreach (_.sender.tell(msg, context.parent))
            stashed = Map.empty

        case FlushWaitingRequests ⇒
            val now = System.nanoTime()
            stashed.values foreach (_.cancel())
            stashed.keys foreach {
                case WaitingRequest(act, req, timeoutRemaining, origTimeout, recvd) ⇒
                    sender() ! StashedRequest(act, req, timeoutRemaining - (now - recvd).nanos, origTimeout)
            }
            stashed = Map.empty
    }
}
