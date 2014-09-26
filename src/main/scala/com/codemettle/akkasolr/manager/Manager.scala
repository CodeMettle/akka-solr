/*
 * Manager.scala
 *
 * Updated: Sep 26, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.manager

import spray.http.Uri

import com.codemettle.akkasolr.Solr
import com.codemettle.akkasolr.client.ClientConnection
import com.codemettle.akkasolr.manager.Manager.Messages.ClientTo
import com.codemettle.akkasolr.manager.Manager.connName

import akka.actor.{Actor, ActorRef, Props}

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

    def receive = {
        case ClientTo(uri, addr) ⇒
            val connection = connections get uri match {
                case Some(c) ⇒ c
                case None ⇒
                    val name = connName.replaceAllIn(uri.toString(), "-")
                    val actor = context.actorOf(Solr.Client.connectionProvider connectionActorProps uri, name)
                    connections += (uri → actor)
                    actor
            }

            sender().tell(Solr.SolrConnection(addr, connection), connection)
    }
}
