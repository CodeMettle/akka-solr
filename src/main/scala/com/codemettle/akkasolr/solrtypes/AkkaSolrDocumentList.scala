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
    def numFound = original.getNumFound
    def start = original.getStart
    def maxScore = Option(original.getMaxScore) map (_.floatValue())

    def documents = original.asScala map AkkaSolrDocument.apply
}
