/*
 * AkkaSolrDocumentList.scala
 *
 * Updated: Sep 23, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.solrtypes

import org.apache.solr.common.SolrDocumentList

import scala.collection.JavaConverters._

/**
 * @author steven
 *
 */
@SerialVersionUID(1L)
case class AkkaSolrDocumentList(original: SolrDocumentList) {
    @transient
    val resultInfo = SolrResultInfo(original.getNumFound, original.getStart, original.getMaxScore)

    def numFound = resultInfo.numFound
    def start = resultInfo.start
    def maxScore = resultInfo.maxScore

    def documents = original.asScala map AkkaSolrDocument.apply
}
