/*
 * Manager.scala
 *
 * Updated: Oct 10, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.manager

import spray.http.Uri

import com.codemettle.akkasolr.Solr
import com.codemettle.akkasolr.Solr.LBConnectionOptions
import com.codemettle.akkasolr.client.LBClientConnection
import com.codemettle.akkasolr.manager.Manager.Messages.{ClientTo, LBClientTo}
import com.codemettle.akkasolr.manager.Manager.connName
import com.codemettle.akkasolr.util.Util

import akka.actor.{Actor, ActorRef, Props, Status}
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
    }

    private val connName = """[^a-zA-Z0-9-]""".r
}

class Manager extends Actor {
    private var connections = Map.empty[Uri, ActorRef]
    private var lbConnections = Map.empty[Set[Uri], ActorRef]

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

    private def getLbConnection(addrs: Map[Uri, String], opts: LBConnectionOptions): ActorRef = {
        lbConnections get addrs.keySet match {
            case Some(c) ⇒ c
            case None ⇒
                val connections = addrs map {
                    case (uri, addr) ⇒ Solr.SolrConnection(addr, getConnection(uri, addr))
                }

                val name = lbNamer.next()
                val actor = context.actorOf(LBClientConnection.props(connections, opts), name)
                lbConnections += (addrs.keySet → actor)
                actor
        }
    }

    def receive = {
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
