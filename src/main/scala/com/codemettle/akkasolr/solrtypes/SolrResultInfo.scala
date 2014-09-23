/*
 * SolrResultInfo.scala
 *
 * Updated: Sep 23, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.solrtypes

/**
 * @author steven
 *
 */
@SerialVersionUID(1L)
case class SolrResultInfo(numFound: Long, start: Long, maxScore: Option[Float])

object SolrResultInfo {
    def apply(numFound: Long, start: Long, maxScore: java.lang.Float): SolrResultInfo = {
        SolrResultInfo(numFound, start, Option(maxScore) map (_.floatValue()))
    }
}
