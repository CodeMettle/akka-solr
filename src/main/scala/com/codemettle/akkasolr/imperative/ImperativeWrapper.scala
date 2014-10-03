/*
 * ImperativeWrapper.scala
 *
 * Updated: Oct 3, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.imperative

import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.params.SolrParams

import com.codemettle.akkasolr.Solr
import com.codemettle.akkasolr.solrtypes.SolrQueryResponse

import akka.actor.ActorRef
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
case class ImperativeWrapper(connection: ActorRef) {
    def call(op: Solr.SolrOperation): Future[SolrQueryResponse] = {
        implicit val timeout = Timeout(op.options.requestTimeout + 5.seconds)

        (connection ? op).mapTo[SolrQueryResponse]
    }

    // Shortcut methods

    def query(q: SolrParams, options: Solr.RequestOptions = Solr.RequestOptions()) = {
        call(Solr.Select(q, options))
    }

    def ping(options: Solr.RequestOptions = Solr
        .RequestOptions(method = Solr.RequestMethods.GET, requestTimeout = 5.seconds)) = {
        call(Solr.Ping(options = options))
    }

    def commit(waitForSearcher: Boolean = true, softCommit: Boolean = false,
               options: Solr.RequestOptions = Solr.RequestOptions()) = {
        call(Solr.Commit(waitForSearcher, softCommit, options))
    }

    def optimize(waitForSearcher: Boolean = true, maxSegments: Int = 1,
                 options: Solr.RequestOptions = Solr.RequestOptions()) = {
        call(Solr.Optimize(waitForSearcher, maxSegments, options))
    }

    def rollback(options: Solr.RequestOptions = Solr.RequestOptions()) = {
        call(Solr.Rollback(options))
    }

    def add(docs: Seq[SolrInputDocument], options: Solr.RequestOptions = Solr.RequestOptions()) = {
        call(Solr.Update AddSolrDocs (docs: _*) withOptions options)
    }

    def deleteById(id: String, options: Solr.RequestOptions = Solr.RequestOptions()) = {
        call(Solr.Update DeleteById id withOptions options)
    }

    def deleteByIds(ids: Seq[String], options: Solr.RequestOptions = Solr.RequestOptions()) = {
        call(Solr.Update DeleteById (ids: _*) withOptions options)
    }

    def deleteByQuery(query: String, options: Solr.RequestOptions = Solr.RequestOptions()) = {
        call(Solr.Update DeleteByQueryString query withOptions options)
    }

    def deleteByQueries(queries: Seq[String], options: Solr.RequestOptions = Solr.RequestOptions()) = {
        call(Solr.Update DeleteByQueryString (queries: _*) withOptions options)
    }
}
