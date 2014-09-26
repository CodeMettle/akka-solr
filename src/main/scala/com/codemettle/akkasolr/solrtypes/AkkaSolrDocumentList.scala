/*
 * AkkaSolrDocumentList.scala
 *
 * Updated: Sep 26, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.solrtypes

import org.apache.solr.common.{SolrDocument, SolrDocumentList}

import scala.collection.JavaConverters._

/**
 * @author steven
 *
 */
@SerialVersionUID(1L)
case class AkkaSolrDocumentList(original: Option[SolrDocumentList]) {
    @transient
    val resultInfo = {
        original.fold(SolrResultInfo(-1, -1, None))(o ⇒ SolrResultInfo(o.getNumFound, o.getStart, o.getMaxScore))
    }

    def numFound = resultInfo.numFound
    def start = resultInfo.start
    def maxScore = resultInfo.maxScore

    @transient
    lazy val documents = original.fold(Seq.empty[SolrDocument])(o ⇒ o.asScala) map AkkaSolrDocument.apply
}
