/*
 * ImperativeWrapper.scala
 *
 * Updated: Oct 3, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr
package imperative

import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.params.SolrParams

import com.codemettle.akkasolr.querybuilder.SolrQueryBuilder
import com.codemettle.akkasolr.solrtypes.SolrQueryResponse

import akka.actor.{ActorRef, ActorRefFactory}
import akka.pattern._
import akka.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Class to ease transitions from other libraries
 *
 * @author steven
 *
 */
case class ImperativeWrapper(connection: ActorRef)(implicit arf: ActorRefFactory) {
    def call(op: Solr.SolrOperation): Future[SolrQueryResponse] = {
        implicit val timeout = Timeout(op.requestTimeout + 5.seconds)

        (connection ? op).mapTo[SolrQueryResponse]
    }

    // Shortcut methods

    def query(q: SolrParams) = {
        call(Solr.Select(q))
    }

    def query(q: SolrQueryBuilder) = {
        call(Solr.Select(q))
    }

    def queryWithOptions(q: SolrParams, options: Solr.RequestOptions) = {
        call(Solr.Select(q, options))
    }

    def queryWithOptions(q: SolrQueryBuilder, options: Solr.RequestOptions) = {
        call(Solr.Select(q) withOptions options)
    }

    def ping(options: Solr.RequestOptions = Solr.RequestOptions(actorSystem).copy(method = Solr.RequestMethods.GET, requestTimeout = 5.seconds)) = {
        call(new Solr.Ping(None, options))
    }

    def commit(waitForSearcher: Boolean = true, softCommit: Boolean = false,
               options: Solr.RequestOptions = Solr.RequestOptions(actorSystem)) = {
        call(Solr.Commit(waitForSearcher, softCommit, options))
    }

    def optimize(waitForSearcher: Boolean = true, maxSegments: Int = 1,
                 options: Solr.RequestOptions = Solr.RequestOptions(actorSystem)) = {
        call(Solr.Optimize(waitForSearcher, maxSegments, options))
    }

    def rollback(options: Solr.RequestOptions = Solr.RequestOptions(actorSystem)) = {
        call(Solr.Rollback(options))
    }

    def add(docs: Seq[SolrInputDocument],
            options: Solr.RequestOptions = Solr.RequestOptions(actorSystem),
            updateOptions: Solr.UpdateOptions = Solr.UpdateOptions(actorSystem)) = {
        call(Solr.Update AddSolrDocs (docs: _*) withOptions options withUpdateOptions updateOptions)
    }

    def deleteById(id: String,
                   options: Solr.RequestOptions = Solr.RequestOptions(actorSystem),
                   updateOptions: Solr.UpdateOptions = Solr.UpdateOptions(actorSystem)) = {
        call(Solr.Update DeleteById id withOptions options withUpdateOptions updateOptions)
    }

    def deleteByIds(ids: Seq[String],
                    options: Solr.RequestOptions = Solr.RequestOptions(actorSystem),
                    updateOptions: Solr.UpdateOptions = Solr.UpdateOptions(actorSystem)) = {
        call(Solr.Update DeleteById (ids: _*) withOptions options withUpdateOptions updateOptions)
    }

    def deleteByQuery(query: String,
                      options: Solr.RequestOptions = Solr.RequestOptions(actorSystem),
                      updateOptions: Solr.UpdateOptions = Solr.UpdateOptions(actorSystem)) = {
        call(Solr.Update DeleteByQueryString query withOptions options withUpdateOptions updateOptions)
    }

    def deleteByQueries(queries: Seq[String],
                        options: Solr.RequestOptions = Solr.RequestOptions(actorSystem),
                        updateOptions: Solr.UpdateOptions = Solr.UpdateOptions(actorSystem)) = {
        call(Solr.Update DeleteByQueryString (queries: _*) withOptions options withUpdateOptions updateOptions)
    }
}
