/*
 * SolrQueryResponse.scala
 *
 * Updated: Sep 23, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.solrtypes

import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.util.NamedList

import com.codemettle.akkasolr.Solr

/**
 * @author steven
 *
 */
@SerialVersionUID(1L)
case class SolrQueryResponse(forRequest: Solr.SolrOperation, original: QueryResponse) {
    @transient
    lazy val header = SolrQueryResponseHeader(original.getHeader)

    @transient
    lazy val results = AkkaSolrDocumentList(original.getResults)
}

object SolrQueryResponse {
    def apply(req: Solr.SolrOperation, nl: NamedList[AnyRef]): SolrQueryResponse = {
        SolrQueryResponse(req, new QueryResponse(nl, null))
    }
}
