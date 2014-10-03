/*
 * Manager.scala
 *
 * Updated: Oct 3, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.manager

import spray.http.Uri

import com.codemettle.akkasolr.Solr
import com.codemettle.akkasolr.client.ClientConnection
import com.codemettle.akkasolr.manager.Manager.Messages.ClientTo
import com.codemettle.akkasolr.manager.Manager.connName
import com.codemettle.akkasolr.util.Util

import akka.actor.{Status, Actor, ActorRef, Props}
import scala.util.{Success, Failure, Try}

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
    }

    private val connName = """[^a-zA-Z0-9-]""".r
}

class Manager extends Actor {
    private var connections = Map.empty[Uri, ActorRef]

    private def getConnection(uri: Uri, addr: String) = {
        connections get uri match {
            case Some(c) ⇒ c
            case None ⇒
                val name = connName.replaceAllIn(uri.toString(), "-")
                val actor = context.actorOf(Solr.Client connectionActorProps uri, name)
                connections += (uri → actor)
                actor
        }
    }

    def receive = {
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
