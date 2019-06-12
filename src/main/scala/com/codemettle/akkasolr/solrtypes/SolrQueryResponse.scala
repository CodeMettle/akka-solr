/*
 * SolrQueryResponse.scala
 *
 * Updated: Oct 3, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.solrtypes

import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.util.NamedList

import com.codemettle.akkasolr.CollectionConverters._
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
    lazy val results = AkkaSolrDocumentList(Option(original.getResults))

    @transient
    lazy val facetFields = Option(original.getFacetFields) map (ffs => {
        (ffs.asScala map (ff => {
            ff.getName -> {
                (ff.getValues.asScala map (c => c.getName -> c.getCount)).toMap
            }
        })).toMap
    })

    @transient
    lazy val facetPivotFields = Option(original.getFacetPivot)

    @transient
    lazy val fieldStatsInfo = Option(original.getFieldStatsInfo)

    @transient
    lazy val groupResponse = original.getGroupResponse

    @transient
    lazy val status = original.getStatus

    @transient
    lazy val qTime = original.getQTime

    @transient
    lazy val errorOpt: Option[NamedList[_]] = Option(original.getResponse.get("error")) collect {
        case nl: NamedList[_] => nl
    }

    @transient
    lazy val errorMessageOpt: Option[String] = errorOpt.flatMap(nl => Option(nl.get("msg"))) collect {
        case s: String => s
    }

    @transient
    lazy val nextCursorMarkOpt = {
        Option(original.getResponse.get(/*CursorMarkParams.CURSOR_MARK_NEXT*/"nextCursorMark").asInstanceOf[String])
    }

    @transient
    lazy val nextCursorMarkUnsafe = nextCursorMarkOpt getOrElse sys.error("No nextCursorMark in response")
}
