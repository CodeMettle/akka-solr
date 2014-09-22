/*
 * TestSolrQueryBuilder.scala
 *
 * Updated: Sep 22, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.client

import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.params.SolrParams
import org.scalatest._

import com.codemettle.akkasolr.Solr
import com.codemettle.akkasolr.querybuilder.SolrQueryBuilder
import com.codemettle.akkasolr.querybuilder.SolrQueryBuilder.{FieldStrToSort, ImmutableSolrParams}

import scala.concurrent.duration.Duration

/**
 * @author steven
 *
 */
object TestSolrQueryBuilder {
    implicit class RichSolrParams(val sp: SolrParams) extends AnyVal {
        def toMap = {
            import scala.collection.JavaConverters._
            (sp.getParameterNamesIterator.asScala map (k ⇒ k → sp.getParams(k).toVector)).toMap
        }
    }
}

class TestSolrQueryBuilder extends FlatSpec with Matchers {
    private def checkEquals(sp1: SolrParams, sp2: SolrParams): Unit = {
        import com.codemettle.akkasolr.client.TestSolrQueryBuilder.RichSolrParams
        sp1.toMap should equal (sp2.toMap)
    }

    private def checkEquals(sp: SolrParams, sqc: SolrQueryBuilder): Unit = checkEquals(sp, sqc.toParams)

    "An ImmutableSolrParams" should "accept a SolrQuery" in {
        val sq = new SolrQuery("*")
        sq.setRows(42)
        sq.setStart(7)
        sq.setFields("f1", "f2")
        sq.addSort("f2", SolrQuery.ORDER.desc)
        sq.addSort("f1", SolrQuery.ORDER.asc)
        sq.addFacetField("f1")
        sq.setTimeAllowed(60000)

        checkEquals(sq, ImmutableSolrParams(sq))
    }

    "A SolrQueryCreator" should "create a query" in {
        val sqc = Solr createQuery "*"

        val sq = new SolrQuery("*")

        checkEquals(sq, sqc)
    }

    it should "add supported fields" in {
        val sqc = Solr createQuery "*" rows 42 start 7 fields("f1", "f2") sortBy("f2".desc, "f1".asc) facets
            "f1" allowedExecutionTime 60000

        val sq = new SolrQuery("*")
        sq.setRows(42)
        sq.setStart(7)
        sq.setFields("f1", "f2")
        sq.addSort("f2", SolrQuery.ORDER.desc)
        sq.addSort("f1", SolrQuery.ORDER.asc)
        sq.addFacetField("f1")
        sq.setTimeAllowed(60000)

        checkEquals(sq, sqc)
    }

    it should "deal with changes" in {
        val sqc = Solr createQuery "*"
        val sq = new SolrQuery("*")

        checkEquals(sq, sqc)

        sq.setFields("a", "b", "c")
        val sqc2 = sqc fields ("a", "b", "c")

        checkEquals(sq, sqc2)

        sq.setFields("a")
        val sqc3 = sqc2 withoutFields ("c", "b")

        checkEquals(sq, sqc3)

        sq.addOrUpdateSort("g", SolrQuery.ORDER.desc)
        sq.addOrUpdateSort("h", SolrQuery.ORDER.asc)
        val sqc4 = sqc3 sortBy ("g".desc, "h".asc)

        checkEquals(sq, sqc4)

        sq.addOrUpdateSort("g", SolrQuery.ORDER.asc)
        sq.addOrUpdateSort("h", SolrQuery.ORDER.desc)
        val sqc5 = sqc4 withSort "g".asc withSort "h".desc

        checkEquals(sq, sqc5)

        sq.setSort("j", SolrQuery.ORDER.asc)
        val sqc6 = sqc5 sortBy "j".asc

        checkEquals(sq, sqc6)

        sq.addFacetField("f1", "f2")
        val sqc7 = sqc6 facets ("f1", "f2")

        checkEquals(sq, sqc7)

        sq.removeFacetField("f1")
        val sqc8 = sqc7 withoutFacetField "f1"

        checkEquals(sq, sqc8)

        sq.setTimeAllowed(25)
        val sqc9 = sqc8 allowedExecutionTime 25

        checkEquals(sq, sqc9)

        sq.setTimeAllowed(null)
        val sqc10 = sqc9 allowedExecutionTime Duration.Undefined

        checkEquals(sq, sqc10)
    }
}
