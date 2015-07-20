/*
 * TestSolrQueryStringBuilder.scala
 *
 * Updated: Oct 14, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.querybuilder

import com.typesafe.config.ConfigFactory
import org.scalatest._

import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder._

import akka.actor.ActorSystem
import akka.testkit.TestKit

/**
 * @author steven
 *
 */
class TestSolrQueryStringBuilder(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers {

    def this() = this(ActorSystem("Test",
        ConfigFactory parseString "akkasolr.solrMaxBooleanClauses=5" withFallback ConfigFactory.load()))

    "A Tree of QueryParts" should "render" in {
        val parts = AndQuery(Seq(
            FieldValue(None, "v"),

            Not(
                OrQuery(
                    Seq(FieldValue(Some("f"), "v"),
                        FieldValue(Some("f2"), "v2"))
                )
            )
        ))

        parts.render should equal ("(v AND -(f:v OR f2:v2))")

        val parts2 = AndQuery(Seq(
            IsAnyOf(Some("any"), List("1", "2")),

            Not(
                OrQuery(
                    Seq(FieldValue(Some("f"), "v"),
                        FieldValue(Some("f2"), "v2"))
                )
            )
        ))

        parts2.render should equal ("(any:(1 OR 2) AND -(f:v OR f2:v2))")

        val parts3 = AndQuery(Seq(
            Not(
                IsAnyOf(Some("any"), List("1", "2"))
            ),

            Not(
                OrQuery(
                    Seq(FieldValue(Some("f"), "v"),
                        FieldValue(Some("f2"), "v2"))
                )
            )
        ))

        parts3.render should equal ("(-any:(1 OR 2) AND -(f:v OR f2:v2))")
    }

    it should "split IsAnyOf requests if they are too large" in {
        val parts = IsAnyOf(Some("f"), List(1, 2, 3, 4, 5))

        parts.render shouldBe a[String]

        parts.copy(values = 0 :: parts.values.toList).render should equal ("(f:(0 OR 1 OR 2 OR 3 OR 4) OR f:(5))")
    }

    it should "have a working DSL" in {
        import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.Methods._

        val p = defaultField() := 3

        p.render should equal ("3")

        val p2 = defaultField() :!= "3"

        p2.render should equal ("-3")

        val p3 = field("id") := "1234"

        p3.render should equal ("id:1234")

        val p4 = field("id") isAnyOf (1, 3, 5)

        p4.render should equal ("id:(1 OR 3 OR 5)")

        val p5 = field("date") isInRange (12345678, 12345679)

        p5.render should equal ("date:[12345678 TO 12345679]")

        val p6 = AND(
            field("blah") := 2,
            field("f2") :!= "ueo",
            defaultField() exists()
        )

        p6.render should equal ("(blah:2 AND -f2:ueo AND [* TO *])")

        val p7 = OR(
            defaultField() isAnyOf (1, 2),
            AND(
                field("f") := 2,
                field("g") doesNotExist()
            )
        )

        p7.render should equal ("((1 OR 2) OR (f:2 AND -g:[* TO *]))")

        val q = AND(
            field("f1") := 3,
            field("f2") :!= "x",
            OR(
                defaultField() isAnyOf(1, 2),
                NOT(field("f3") := 4),
                field("time") isInRange(3, 5)
            )
        )

        q.render should equal ("(f1:3 AND -f2:x AND ((1 OR 2) OR -f3:4 OR time:[3 TO 5]))")
    }

    "Empty" should "be left out of AND and OR queries" in {
        import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.Methods._

        val q = AND (
            Empty,
            defaultField() := "*"
        )

        q.render should equal ("*")

        val q2 = AND (
            Empty,
            defaultField() := "*",
            Empty,
            field("a") := "b"
        )

        q2.render should equal ("(* AND a:b)")

        val q3 = OR (
            defaultField() := "*",
            Empty
        )

        q3.render should equal ("*")

        val q4 = OR (
            defaultField() := "*",
            Empty,
            field("a") := "b",
            Empty
        )

        q4.render should equal ("(* OR a:b)")
    }

    "isAnyOf" should "be empty if no values supplied" in {
        import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.Methods._

        val q = field("blah") isAnyOf (Nil: _*)

        q should equal (Empty)
    }

    "IsAnyOf" should "render correctly if empty" in {
        IsAnyOf(Some("blah"), Nil).render should be ('empty)
    }

    "NOT" should "render correctly if empty" in {
        import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.Methods._

        val qs = List(NOT(Empty), NOT(rawQuery("")), NOT(IsAnyOf(Some("blah"), Nil)), NOT(OR(Nil: _*)), NOT(AND(Nil: _*)), NOT(field("blah") isAnyOf(Nil: _*)))

        qs map (_.render) foreach (_ should equal (""))
    }

    "isNoneOf" should "render correctly" in {
        import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.Methods._

        (defaultField() isNoneOf (1, 2, 3)).render should equal ("-(1 OR 2 OR 3)")

        (field("x") isNoneOf ("a", "b", "c")).render should equal ("-x:(a OR b OR c)")

        (field("z") isNoneOf (Nil: _*)).render should be ('empty)
    }
}
