package com.github.akkasolr.ext

import com.github.akkasolr.ext.SolrExtImpl.scheme
import com.github.akkasolr.manager.Manager
import com.github.akkasolr.util.Util

import akka.actor.{ActorRef, ExtendedActorSystem, Extension}

/**
 * @author steven
 *
 */
object SolrExtImpl {
    private val scheme = """^https?$""".r
}

class SolrExtImpl(eas: ExtendedActorSystem) extends Extension {
    private val manager = eas.actorOf(Manager.props, "Solr")

    val responseParserDispatcher = eas.dispatchers lookup "akka.solr.response-parser-dispatcher"

    def clientTo(solrUrl: String)(implicit requestor: ActorRef) = {
        val uri = Util normalize solrUrl
        uri.scheme match {
            case scheme() ⇒
            case _ ⇒ sys.error(s"${uri.scheme} connections not supported")
        }
        manager.tell(Manager.Messages.ClientTo(uri, solrUrl), requestor)
    }
}
