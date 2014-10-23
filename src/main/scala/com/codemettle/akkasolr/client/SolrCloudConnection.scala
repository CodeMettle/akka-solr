/*
 * SolrCloudConnection.scala
 *
 * Updated: Oct 23, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.client

import org.apache.solr.common.cloud.ZkStateReader
import org.apache.solr.common.util.NamedList

import com.codemettle.akkasolr.Solr
import com.codemettle.akkasolr.client.SolrCloudConnection.{Connect, OperateOnCollection, fsm}
import com.codemettle.akkasolr.client.zk.{ZkUpdateUtil, ZkRequestHandler, ZkUtil}
import com.codemettle.akkasolr.util.Util

import akka.actor._
import akka.pattern._
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Try}

/**
 * @author steven
 *
 */
object SolrCloudConnection {
    def props(lbConnection: ActorRef, zkHost: String, config: Solr.SolrCloudConnectionOptions) = {
        Props(new SolrCloudConnection(lbConnection, zkHost, config))
    }

    @SerialVersionUID(1L)
    case class OperateOnCollection(op: Solr.SolrOperation, collection: String)

    @SerialVersionUID(1L)
    case class RouteResponse(routeResponses: Map[String, NamedList[AnyRef]],
                             routes: Map[String, LBClientConnection.ExtendedRequest]) extends NamedList[AnyRef]

    object fsm {
        sealed trait State
        case object NotConnected extends State
        case object Connecting extends State
        case object Connected extends State

        case class Data(zkStateReader: ZkStateReader = null)
    }

    private case object Connect
}

class SolrCloudConnection(lbServer: ActorRef, zkHost: String, config: Solr.SolrCloudConnectionOptions)
    extends FSM[fsm.State, fsm.Data] with ActorLogging {

    private val zkUtil = ZkUtil(config)
    private val zkUpdateUtil = ZkUpdateUtil(config)

    private val stasher = context.actorOf(ConnectingStasher.props, "stasher")

    private val actorName = Util actorNamer "request"

    startWith(fsm.NotConnected, fsm.Data())

    override def preStart() = {
        super.preStart()

        if (config.connectAtStart)
            self ! Connect
    }

    onTermination {
        case StopEvent(_, _, data) ⇒ Try(Option(data.zkStateReader) foreach (_.close())) match {
            case Failure(t) ⇒ log.error(t, "Error closing ZkStateReader")
            case _ ⇒
        }
    }

    private def serviceRequest(replyTo: ActorRef, zkStateReader: ZkStateReader, request: Any,
                               timeout: FiniteDuration, origTimeout: FiniteDuration) = {
        val name = actorName.next()
        val props = ZkRequestHandler.props(lbServer, zkStateReader, zkUtil, zkUpdateUtil, config.defaultCollection,
            replyTo, timeout, origTimeout)

        val reqHandler = context.actorOf(props, name)

        reqHandler ! request

        stay()
    }

    private def stash(op: Any, replyTo: ActorRef) = {
        val timeout = op match {
            case OperateOnCollection(so, _) ⇒ so.requestTimeout
            case so: Solr.SolrOperation ⇒ so.requestTimeout
        }

        stasher ! ConnectingStasher.WaitingRequest(replyTo, op, timeout, timeout)
    }

    private def initiateConnection(msgToStash: Option[Any]) = {
        implicit val dispatcher = context.dispatcher

        msgToStash foreach (s ⇒ stash(s, sender()))

        (zkUtil connect zkHost) pipeTo self
        goto(fsm.Connecting)
    }

    when(fsm.NotConnected) {
        case Event(Connect, _) ⇒ initiateConnection(None)
        case Event(op@(_: OperateOnCollection | _: Solr.SolrOperation), _) ⇒ initiateConnection(Some(op))
    }

    when(fsm.Connecting) {
        case Event(reader: ZkStateReader, _) ⇒ goto(fsm.Connected) using fsm.Data(reader)
        case Event(Status.Failure(t), _) ⇒
            log.error(t, "Error creating ZkStateReader, retrying")
            initiateConnection(None)

        case Event(op@(_: OperateOnCollection | _: Solr.SolrOperation), _) ⇒
            stash(op, sender())
            stay()
    }

    onTransition {
        case fsm.Connecting -> fsm.Connected ⇒ stasher ! ConnectingStasher.FlushWaitingRequests
    }

    when(fsm.Connected) {
        case Event(ConnectingStasher.StashedRequest(replyTo, req: Solr.SolrOperation, timeout, origTimeout), fsm.Data(reader)) ⇒
            serviceRequest(replyTo, reader, req, timeout, origTimeout)

        case Event(ConnectingStasher.StashedRequest(replyTo, req: OperateOnCollection, timeout, origTimeout), fsm.Data(reader)) ⇒
            serviceRequest(replyTo, reader, req, timeout, origTimeout)

        case Event(req@OperateOnCollection(op, _), fsm.Data(reader)) ⇒
            serviceRequest(sender(), reader, req, op.requestTimeout, op.requestTimeout)

        case Event(op: Solr.SolrOperation, fsm.Data(reader)) ⇒
            serviceRequest(sender(), reader, op, op.requestTimeout, op.requestTimeout)
    }

    initialize()
}
