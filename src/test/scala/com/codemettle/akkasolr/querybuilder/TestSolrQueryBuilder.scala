/*
 * TestSolrQueryBuilder.scala
 *
 * Updated: Oct 2, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.querybuilder

import org.apache.solr.client.solrj.SolrQuery
import org.scalatest._

import com.codemettle.akkasolr.querybuilder.SolrQueryBuilder.FieldStrToSort

/**
 * @author steven
 *
 */
class TestSolrQueryBuilder extends FlatSpec with Matchers with OptionValues {
    "A SolrQueryBuilder" should "be creatable from a SolrQuery" in {
        val q = new SolrQuery("*")

        val sqb1 = SolrQueryBuilder.fromSolrQuery(q)

        sqb1.query should equal ("*")
        sqb1.rowsOpt should be ('empty)
        sqb1.fieldList should be ('empty)
        sqb1.startOpt should be ('empty)
        sqb1.sortsList should be ('empty)
        sqb1.facetFields should be ('empty)
        sqb1.serverTimeAllowed should be ('empty)

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

        val sqb7 = SolrQueryBuilder.fromSolrQuery(q)

        sqb7.query should equal ("*")
        sqb7.rowsOpt.value should equal (10)
        sqb7.fieldList should equal (Vector("a", "b"))
        sqb7.startOpt.value should equal (25)
        sqb7.sortsList should equal (Vector("a".ascending, "b".descending))
        sqb7.facetFields should equal (Vector("a", "b"))
        sqb7.serverTimeAllowed.value should equal (5000)
    }
}
