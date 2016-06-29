/*
 * TestSolrQueryBuilder.scala
 *
 * Updated: Oct 13, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.querybuilder

import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.params.{GroupParams ⇒ SolrGroupParams, CommonParams, ShardParams, StatsParams, SolrParams}
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
        def fq = {
            import SolrQueryStringBuilder.Methods._

            AND(defaultField() := "x", field("f3") isInRange (1, 5))
        }

        val sqc = Solr createQuery "*" rows 42 start 7 fields("f1", "f2") sortBy("f2".desc, "f1".asc) facets
            "f1" allowedExecutionTime 60000 withFacetLimit 10 withFacetMinCount 1 withFacetPrefix
            "testing" withFacetPivotFields "f2,f1" withGroupField "f1" withGroupSorts
            ("f1".asc, "f2".desc) withGroupFormat "simple" groupInMain true groupTotalCount true truncateGroupings
            true withStatsFields Seq("f1","f2") withStatsFacetFields Seq("f2","f1") withGroupLimit
            100 withShards ("shard1", "shard2") withFilterQuery "f1:v1" withFilterQuery fq

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
        sq.addFacetPivotField("f2,f1")
        sq.add(SolrGroupParams.GROUP, "true")
        sq.add(SolrGroupParams.GROUP_FIELD, "f1")
        sq.add(SolrGroupParams.GROUP_SORT, "f1 asc,f2 desc")
        sq.add(SolrGroupParams.GROUP_FORMAT, "simple")
        sq.add(SolrGroupParams.GROUP_MAIN, "true")
        sq.add(SolrGroupParams.GROUP_TOTAL_COUNT, "true")
        sq.add(SolrGroupParams.GROUP_TRUNCATE, "true")
        sq.add(SolrGroupParams.GROUP_LIMIT, "100")
        sq.add(StatsParams.STATS, "true")
        sq.add(StatsParams.STATS_FIELD, "f1")
        sq.add(StatsParams.STATS_FIELD, "f2")
        sq.add(StatsParams.STATS_FACET, "f2")
        sq.add(StatsParams.STATS_FACET, "f1")
        sq.set(ShardParams.SHARDS, "shard1,shard2")
        sq.addFilterQuery("f1:v1", "(x AND f3:[1 TO 5])")

        checkEquals(sq, sqc)
    }

    it should "add supported fields with cursor" in {
        val sqc = Solr createQuery "*" rows 42 beginCursor() fields("f1", "f2") sortBy("f2".desc, "f1".asc) facets
            "f1" allowedExecutionTime 60000 withFacetLimit 10 withFacetMinCount 1 withFacetPrefix
            "testing" withFacetPivotFields "f2,f1" withGroupField "f1" withGroupSorts
            ("f1".asc, "f2".desc) withGroupFormat "simple" groupInMain true groupTotalCount true truncateGroupings
            true withStatsFields Seq("f1","f2") withStatsFacetFields Seq("f2","f1") withGroupLimit
            100 withShards ("shard1", "shard2")

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
        sq.addFacetPivotField("f2,f1")
        sq.add(SolrGroupParams.GROUP, "true")
        sq.add(SolrGroupParams.GROUP_FIELD, "f1")
        sq.add(SolrGroupParams.GROUP_SORT, "f1 asc,f2 desc")
        sq.add(SolrGroupParams.GROUP_FORMAT, "simple")
        sq.add(SolrGroupParams.GROUP_MAIN, "true")
        sq.add(SolrGroupParams.GROUP_TOTAL_COUNT, "true")
        sq.add(SolrGroupParams.GROUP_TRUNCATE, "true")
        sq.add(SolrGroupParams.GROUP_LIMIT, "100")
        sq.add(StatsParams.STATS, "true")
        sq.add(StatsParams.STATS_FIELD, "f1")
        sq.add(StatsParams.STATS_FIELD, "f2")
        sq.add(StatsParams.STATS_FACET, "f2")
        sq.add(StatsParams.STATS_FACET, "f1")
        sq.add(ShardParams.SHARDS, "shard1,shard2")

        checkEquals(sq, sqc)
    }

    it should "disallow start+cursor" in {
        the[IllegalArgumentException] thrownBy (Solr createQuery "*" start 5 beginCursor()) should have message
            "'start' and 'cursorMark' options are mutually exclusive"

        the[IllegalArgumentException] thrownBy (Solr createQuery "*" withCursorMark "abc" start 5) should have message
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

        sq.set(ShardParams.SHARDS, "shard1,shard2")
        val sqb11 = sqc10 shards ("shard1", "shard2")

        checkEquals(sq, sqb11)

        sq.set(ShardParams.SHARDS, "shard2")
        val sqb12 = sqb11 withoutShard "shard1"

        checkEquals(sq, sqb12)

        sq.setFilterQueries("f:v", "f2:v2")
        val sqb13 = {
            import SolrQueryStringBuilder.Methods._
            sqb12 withFilterQueries Seq(field("f") := "v", field("f2") := "v2")
        }

        checkEquals(sq, sqb13)

        sq.addFilterQuery("z:3")
        val sqb14 = sqb13 withFilterQuery "z:3"

        checkEquals(sq, sqb14)

        sq.remove(CommonParams.FQ)
        val sqb15 = sqb14.withoutFilterQueries()

        checkEquals(sq, sqb15)
    }

    it should "be creatable from a SolrQuery" in {
        val q = new SolrQuery("*")

        val sqb1 = SolrQueryBuilder.fromSolrQuery(q)

        sqb1.query should equal (RawQuery("*"))
        sqb1.rowsOpt should be ('empty)
        sqb1.fieldList should be ('empty)
        sqb1.startOpt should be ('empty)
        sqb1.sortsList should be ('empty)
        sqb1.facetParams.fields should be ('empty)
        sqb1.serverTimeAllowed should be ('empty)
        sqb1.facetParams.limit should be ('empty)
        sqb1.facetParams.minCount should be ('empty)
        sqb1.facetParams.prefix should be ('empty)
        sqb1.groupParams.field should be ('empty)
        sqb1.groupParams.sortsList should be ('empty)
        sqb1.groupParams.format should be ('empty)
        sqb1.groupParams.main should be ('empty)
        sqb1.groupParams.totalCount should be ('empty)
        sqb1.groupParams.truncate should be ('empty)
        sqb1.shardList should be ('empty)

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

        sqb6.facetParams.fields should equal (Vector("a", "b"))

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
        sqb7.serverTimeAllowed.value should equal (5000)
        sqb7.facetParams.fields should equal (Vector("a", "b"))
        sqb7.facetParams.limit.value should equal (42)
        sqb7.facetParams.minCount.value should equal (12)
        sqb7.facetParams.prefix.value should equal ("fp")

        q.setStart(null)
        q.set(/*CursorMarkParams.CURSOR_MARK_PARAM*/"cursorMark", "abc")

        val sqb8 = SolrQueryBuilder.fromSolrQuery(q)

        sqb8.startOpt should be ('empty)
        sqb8.cursorMarkOpt.value should equal ("abc")

        q.add(SolrGroupParams.GROUP, "true")
        q.add(SolrGroupParams.GROUP_FIELD, "f1")
        q.add(SolrGroupParams.GROUP_SORT, "f1 asc,f2 desc")
        q.add(SolrGroupParams.GROUP_FORMAT, "simple")
        q.add(SolrGroupParams.GROUP_MAIN, "true")
        q.add(SolrGroupParams.GROUP_TOTAL_COUNT, "true")
        q.add(SolrGroupParams.GROUP_TRUNCATE, "true")

        val sqb9 = SolrQueryBuilder.fromSolrQuery(q)

        sqb9.groupParams.field.value should equal("f1")
        sqb9.groupParams.sortsList should equal(Vector("f1".asc, "f2".descending))
        sqb9.groupParams.format.value should equal("simple")
        sqb9.groupParams.main.value should equal(true)
        sqb9.groupParams.totalCount.value should equal(true)
        sqb9.groupParams.truncate.value should equal(true)

        q.set(ShardParams.SHARDS, "shard1,shard2,shard3")

        val sqb10 = SolrQueryBuilder.fromSolrQuery(q)

        sqb10.shardList should equal (Vector("shard1", "shard2", "shard3"))

        q.addFilterQuery("f:v")

        val sqb11 = SolrQueryBuilder.fromSolrQuery(q)

        sqb11.filterQueries should equal (Vector(RawQuery("f:v")))

        q.addFilterQuery("(z:3 AND x:[1 TO 5])", "blegh:blah")

        val sqb12 = SolrQueryBuilder.fromSolrQuery(q)

        sqb12.filterQueries should equal (Vector(RawQuery("f:v"), RawQuery("(z:3 AND x:[1 TO 5])"), RawQuery("blegh:blah")))
    }
}
