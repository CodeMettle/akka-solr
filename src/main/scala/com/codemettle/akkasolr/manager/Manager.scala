/*
 * Manager.scala
 *
 * Updated: Oct 16, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr
package manager

import spray.http.Uri

import com.codemettle.akkasolr.Solr.{LBConnectionOptions, SolrCloudConnectionOptions}
import com.codemettle.akkasolr.client.{LBClientConnection, SolrCloudConnection}
import com.codemettle.akkasolr.manager.Manager.Messages.{SolrCloudClientTo, ClientTo, LBClientTo}
import com.codemettle.akkasolr.manager.Manager.connName
import com.codemettle.akkasolr.util.Util

import akka.actor._
import scala.util.{Failure, Success, Try}

/**
 * @author steven
 *
 */
object Manager {
    val props = {
        Props[Manager]
    }

    object Messages {
        case class ClientTo(uri: Uri, addr: String)
        case class LBClientTo(addrs: Map[Uri, String], orig: Set[String], opts: LBConnectionOptions)
        case class SolrCloudClientTo(zkHost: String, opts: SolrCloudConnectionOptions)
    }

    private val connName = """[^a-zA-Z0-9-]""".r
}

class Manager extends Actor {
    private var connections = Map.empty[Uri, ActorRef]
    private var lbConnections = Map.empty[Set[Uri], ActorRef]
    private var solrCloudConnections = Map.empty[String, (ActorRef, ActorRef)]

    private val lbNamer = Util actorNamer "loadBalancer"

    private def getConnection(uri: Uri, addr: String): ActorRef = {
        connections get uri match {
            case Some(c) ⇒ c
            case None ⇒
                val name = connName.replaceAllIn(uri.toString(), "-")
                val actor = context.actorOf(Solr.Client connectionActorProps uri, name)
                connections += (uri → actor)
                actor
        }
    }

    private def createLbConnection(conns: Iterable[Solr.SolrConnection], opts: LBConnectionOptions) = {
        val name = lbNamer.next()
        context.actorOf(LBClientConnection.props(conns, opts), name)
    }

    private def getLbConnection(addrs: Map[Uri, String], opts: LBConnectionOptions): ActorRef = {
        lbConnections get addrs.keySet match {
            case Some(c) ⇒ c
            case None ⇒
                val connections = addrs map {
                    case (uri, addr) ⇒ Solr.SolrConnection(addr, getConnection(uri, addr))
                }

                val actor = createLbConnection(connections, opts)
                lbConnections += (addrs.keySet → actor)
                actor
        }
    }

    private def getSolrCloudConnection(zkHost: String, opts: SolrCloudConnectionOptions): ActorRef = {
        solrCloudConnections get zkHost match {
            case Some((c, _)) ⇒ c
            case None ⇒
                val name = connName.replaceAllIn(zkHost, "-")
                val lbServer = createLbConnection(Nil, LBConnectionOptions(actorSystem))
                val actor = context.actorOf(SolrCloudConnection.props(lbServer, zkHost, opts), name)
                context watch actor
                solrCloudConnections += (zkHost → (actor → lbServer))
                actor
        }
    }

    def receive = {
        case Terminated(act) ⇒ solrCloudConnections find (_._2._1 == act) foreach {
            case (zkHost, (_, lbServer)) ⇒
                context stop lbServer
                solrCloudConnections -= zkHost
        }

        case SolrCloudClientTo(zkHost, opts) ⇒
            val connection = getSolrCloudConnection(zkHost, opts)

            sender().tell(Solr.SolrConnection(zkHost, connection), connection)

        case LBClientTo(addrs, orig, opts) ⇒
            val connection = getLbConnection(addrs, opts)

            sender().tell(Solr.SolrLBConnection(orig, connection), connection)

        case ClientTo(uri, addr) ⇒
            val connection = getConnection(uri, addr)

            sender().tell(Solr.SolrConnection(addr, connection), connection)

        case Solr.Request(addr, op) ⇒
            Try(Util normalize addr) match {
                case Failure(t) ⇒ sender() ! Status.Failure(t)
                case Success(uri) ⇒ getConnection(uri, addr) forward op
            }
    }
}
