package com.github.akkasolr

import com.github.akkasolr.ext.SolrExtImpl

import akka.actor._

/**
 * @author steven
 *
 */
object Solr extends ExtensionId[SolrExtImpl] with ExtensionIdProvider {
    def Client(implicit arf: ActorRefFactory) = actorSystem registerExtension this

    override def createExtension(system: ExtendedActorSystem) = new SolrExtImpl(system)

    override def lookup() = Solr

    /***** messages *****/
    case class SolrConnection(forAddress: String, connection: ActorRef)
}
