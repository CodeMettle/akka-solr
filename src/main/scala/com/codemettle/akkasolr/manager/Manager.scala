/*
 * Manager.scala
 *
 * Updated: Nov 20, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr
package manager

import com.codemettle.akkasolr.Solr.{LBConnectionOptions, SolrCloudConnectionOptions}
import com.codemettle.akkasolr.client.{LBClientConnection, SolrCloudConnection}
import com.codemettle.akkasolr.manager.Manager.Messages.{ClientTo, LBClientTo, SolrCloudClientTo}
import com.codemettle.akkasolr.manager.Manager.{ConnKey, connName}
import com.codemettle.akkasolr.util.Util

import akka.actor._
import akka.http.scaladsl.model.Uri
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
        case class ClientTo(uri: Uri, addr: String, username: Option[String], password: Option[String])
        case class LBClientTo(addrs: Map[Uri, String], orig: Set[String], opts: LBConnectionOptions)
        case class SolrCloudClientTo(zkHost: String, opts: SolrCloudConnectionOptions)
    }

    private val connName = """[^a-zA-Z0-9-]""".r

    private case class ConnKey(uri: Uri, user: Option[String], pass: Option[String])
}

class Manager extends Actor {
    private var connections = Map.empty[ConnKey, ActorRef]
    private var lbConnections = Map.empty[(Set[ConnKey], LBConnectionOptions), ActorRef]
    private var solrCloudConnections = Map.empty[(String, SolrCloudConnectionOptions), ActorRef]

    private val lbNamer = Util actorNamer "loadBalancer"
    private val zkNamer = Util actorNamer "zkClient"

    private def getConnection(key: ConnKey, addr: String): ActorRef = {
        connections get key match {
            case Some(c) => c
            case None =>
                val name = connName.replaceAllIn(key.uri.toString(), "-")
                val actor = context.actorOf(Solr.Client.connectionActorProps(key.uri, key.user, key.pass), name)
                connections += (key -> actor)
                actor
        }
    }

    private def createLbConnection(conns: Iterable[Solr.SolrConnection], opts: LBConnectionOptions) = {
        val name = lbNamer.next()
        context.actorOf(LBClientConnection.props(conns, opts), name)
    }

    private def getLbConnection(addrs: Map[Uri, String], opts: LBConnectionOptions): ActorRef = {
        val connKeys = addrs.keySet.map(ConnKey(_, None, None))

        lbConnections get (connKeys -> opts) match {
            case Some(c) => c
            case None =>
                val connections = addrs map {
                    case (uri, addr) => Solr.SolrConnection(addr, getConnection(ConnKey(uri, None, None), addr))
                }

                val actor = createLbConnection(connections, opts)
                lbConnections += ((connKeys -> opts) -> actor)
                actor
        }
    }

    private def getSolrCloudConnection(zkHost: String, opts: SolrCloudConnectionOptions): ActorRef = {
        solrCloudConnections get (zkHost -> opts) match {
            case Some(c) => c
            case None =>
                val actor = context.actorOf(SolrCloudConnection.props(zkHost, opts), zkNamer.next())
                context watch actor
                solrCloudConnections += ((zkHost -> opts) -> actor)
                actor
        }
    }

    def receive = {
        case Terminated(act) => solrCloudConnections find (_._2 == act) foreach {
            case (hostAndOpts, _) => solrCloudConnections -= hostAndOpts
        }

        case SolrCloudClientTo(zkHost, opts) =>
            val connection = getSolrCloudConnection(zkHost, opts)

            sender().tell(Solr.SolrConnection(zkHost, connection), connection)

        case LBClientTo(addrs, orig, opts) =>
            val connection = getLbConnection(addrs, opts)

            sender().tell(Solr.SolrLBConnection(orig, connection), connection)

        case ClientTo(uri, addr, user, pass) =>
            val connection = getConnection(ConnKey(uri, user, pass), addr)

            sender().tell(Solr.SolrConnection(addr, connection), connection)

        case Solr.Request(addr, op, user, pass) =>
            Try(Util normalize addr) match {
                case Failure(t) => sender() ! Status.Failure(t)
                case Success(uri) => getConnection(ConnKey(uri, user, pass), addr) forward op
            }
    }
}
