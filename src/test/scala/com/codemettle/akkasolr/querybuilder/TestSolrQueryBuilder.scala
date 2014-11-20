/*
 * TestSolrQueryBuilder.scala
 *
 * Updated: Oct 13, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.querybuilder

import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.params.SolrParams
import org.scalatest._

import com.codemettle.akkasolr.Solr
import com.codemettle.akkasolr.querybuilder.SolrQueryBuilder.{ImmutableSolrParams, FieldStrToSort}
import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.RawQuery

import akka.actor.ActorSystem
import akka.testkit.TestKit
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

class TestSolrQueryBuilder(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with OptionValues {
    def this() = this(ActorSystem("Test"))

    private def checkEquals(sp1: SolrParams, sp2: SolrParams): Unit = {
        import com.codemettle.akkasolr.querybuilder.TestSolrQueryBuilder.RichSolrParams
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

    "A SolrQueryBuilder" should "create a query" in {
        val sqc = Solr createQuery "*"

        val sq = new SolrQuery("*")

        checkEquals(sq, sqc)
    }

    it should "add supported fields" in {
        val sqc = Solr createQuery "*" rows 42 start 7 fields("f1", "f2") sortBy("f2".desc, "f1".asc) facets
            "f1" allowedExecutionTime 60000 withFacetLimit 10 withFacetMinCount 1 withFacetPrefix "testing"

        val sq = new SolrQuery("*")
        sq.setRows(42)
        sq.setStart(7)
        sq.setFields("f1", "f2")
        sq.addSort("f2", SolrQuery.ORDER.desc)
        sq.addSort("f1", SolrQuery.ORDER.asc)
        sq.addFacetField("f1")
        sq.setTimeAllowed(60000)
        sq.setFacetLimit(10)
        sq.setFacetMinCount(1)
        sq.setFacetPrefix("testing")

        checkEquals(sq, sqc)
    }

    it should "add supported fields with cursor" in {
        val sqc = Solr createQuery "*" rows 42 beginCursor() fields("f1", "f2") sortBy("f2".desc, "f1".asc) facets
            "f1" allowedExecutionTime 60000 withFacetLimit 10 withFacetMinCount 1 withFacetPrefix "testing"

        val sq = new SolrQuery("*")
        sq.setRows(42)
        sq.set(/*CursorMarkParams.CURSOR_MARK_PARAM*/"cursorMark", "*")
        sq.setFields("f1", "f2")
        sq.addSort("f2", SolrQuery.ORDER.desc)
        sq.addSort("f1", SolrQuery.ORDER.asc)
        sq.addFacetField("f1")
        sq.setTimeAllowed(60000)
        sq.setFacetLimit(10)
        sq.setFacetMinCount(1)
        sq.setFacetPrefix("testing")

        checkEquals(sq, sqc)
    }

    it should "disallow start+cursor" in {
        the[IllegalArgumentException] thrownBy (Solr createQuery "*" start 5 beginCursor()) should have message
            "'start' and 'cursorMark' options are mutually exclusive"

        the[IllegalArgumentException] thrownBy (Solr createQuery "*" cursorMark "abc" start 5) should have message
            "'start' and 'cursorMark' options are mutually exclusive"
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

    it should "be creatable from a SolrQuery" in {
        val q = new SolrQuery("*")

        val sqb1 = SolrQueryBuilder.fromSolrQuery(q)

        sqb1.query should equal (RawQuery("*"))
        sqb1.rowsOpt should be ('empty)
        sqb1.fieldList should be ('empty)
        sqb1.startOpt should be ('empty)
        sqb1.sortsList should be ('empty)
        sqb1.facetFields should be ('empty)
        sqb1.serverTimeAllowed should be ('empty)
        sqb1.facetLimit should be ('empty)
        sqb1.facetMinCount should be ('empty)
        sqb1.facetPrefix should be ('empty)

        q setRows 10

        val sqb2 = SolrQueryBuilder.fromSolrQuery(q)

        sqb2.rowsOpt.value should equal (10)

        q.setFields("a", "b")

        val sqb3 = SolrQueryBuilder.fromSolrQuery(q)

        sqb3.fieldList should equal (Vector("a", "b"))

        q setStart 25

        val sqb4 = SolrQueryBuilder.fromSolrQuery(q)

        sqb4.startOpt.value should equal (25)

        q.addSort("a", SolrQuery.ORDER.asc)
        q.addSort("b", SolrQuery.ORDER.desc)

        val sqb5 = SolrQueryBuilder.fromSolrQuery(q)

        sqb5.sortsList should equal (Vector("a".ascending, "b".descending))

        q.addFacetField("a", "b")

        val sqb6 = SolrQueryBuilder.fromSolrQuery(q)

        sqb6.facetFields should equal (Vector("a", "b"))

        q.setTimeAllowed(5000)
        q.setFacetLimit(42)
        q.setFacetMinCount(12)
        q.setFacetPrefix("fp")

        val sqb7 = SolrQueryBuilder.fromSolrQuery(q)

        sqb7.query should equal (RawQuery("*"))
        sqb7.rowsOpt.value should equal (10)
        sqb7.fieldList should equal (Vector("a", "b"))
        sqb7.startOpt.value should equal (25)
        sqb7.sortsList should equal (Vector("a".ascending, "b".descending))
        sqb7.facetFields should equal (Vector("a", "b"))
        sqb7.serverTimeAllowed.value should equal (5000)
        sqb7.facetLimit.value should equal (42)
        sqb7.facetMinCount.value should equal (12)
        sqb7.facetPrefix.value should equal ("fp")

        q.setStart(null)
        q.set(/*CursorMarkParams.CURSOR_MARK_PARAM*/"cursorMark", "abc")

        val sqb8 = SolrQueryBuilder.fromSolrQuery(q)

        sqb8.startOpt should be ('empty)
        sqb8.cursorMarkOpt.value should equal ("abc")
    }
}
