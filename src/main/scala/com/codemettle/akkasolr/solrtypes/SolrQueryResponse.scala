/*
 * SolrQueryResponse.scala
 *
 * Updated: Sep 26, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.solrtypes

import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.util.NamedList

import com.codemettle.akkasolr.Solr

import scala.collection.JavaConverters._

/**
 * @author steven
 *
 */
@SerialVersionUID(1L)
case class SolrQueryResponse(forRequest: Solr.SolrOperation, original: QueryResponse) {
    @transient
    lazy val header = SolrQueryResponseHeader(original.getHeader)

    @transient
    lazy val results = AkkaSolrDocumentList(Option(original.getResults))

    @transient
    lazy val facetFields = Option(original.getFacetFields) map (ffs ⇒ {
        (ffs.asScala map (ff ⇒ {
            ff.getName → {
                (ff.getValues.asScala map (c ⇒ c.getName → c.getCount)).toMap
            }
        })).toMap
    })
}
