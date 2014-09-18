package com.github.akkasolr.manager

import com.github.akkasolr.Solr
import com.github.akkasolr.client.ClientConnection
import com.github.akkasolr.manager.Manager.Messages.ClientTo
import com.github.akkasolr.manager.Manager.connName
import spray.http.Uri

import akka.actor.{ActorRef, Props, Actor}

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
                    val actor = context.actorOf(ClientConnection props uri, name)
                    connections += (uri → actor)
                    actor
            }

            sender().tell(Solr.SolrConnection(addr, connection), connection)
    }
}
