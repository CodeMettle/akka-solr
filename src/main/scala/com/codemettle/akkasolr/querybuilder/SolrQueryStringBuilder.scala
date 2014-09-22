/*
 * SolrQueryStringBuilder.scala
 *
 * Updated: Sep 22, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.querybuilder

import com.codemettle.akkasolr.Solr

import akka.actor.ActorRefFactory

/**
 * @author steven
 *
 */
object SolrQueryStringBuilder {
    type FieldValueType = Any

    sealed trait QueryPart

    case class FieldBuilder(field: Option[String]) {
        def :=(v: FieldValueType) = FieldValue(field, v)
        def :!=(v: FieldValueType) = Not(FieldValue(field, v))
        def isAnyOf(vs: Iterable[FieldValueType]): QueryPart = isAnyOf(vs.toSeq: _*)
        def isAnyOf(vs: FieldValueType*) = IsAnyOf(field, vs)
        def isInRange(lower: FieldValueType, upper: FieldValueType) = Range(field, lower, upper)
        def exists() = isInRange("*", "*")
        def doesNotExist() = Not(Range(field, "*", "*"))
    }

    trait BuilderMethods {
        def field(f: String) = FieldBuilder(Some(f))
        def defaultField() = FieldBuilder(None)
        def rawQuery(f: String) = RawQuery(f)

        def AND(qps: QueryPart*) = AndQuery(qps)
        def OR(qps: QueryPart*) = OrQuery(qps)
        def NOT(qp: QueryPart) = Not(qp)
    }

    object Methods extends BuilderMethods

    case class OrQuery(parts: Seq[QueryPart]) extends QueryPart

    case class AndQuery(parts: Seq[QueryPart]) extends QueryPart

    case class IsAnyOf(field: Option[String], values: Iterable[FieldValueType]) extends QueryPart

    case class RawQuery(qStr: String) extends QueryPart

    case class FieldValue(field: Option[String], value: FieldValueType) extends QueryPart

    case class Range(field: Option[String], lower: FieldValueType, upper: FieldValueType) extends QueryPart

    case class Not(qp: QueryPart) extends QueryPart

    case object Empty extends BuilderMethods

    private def valueEsc(value: FieldValueType): String = value match {
        case s: String ⇒
            if (s.contains(" "))
                '"' + s + '"'
            else s

        case _ ⇒ value.toString
    }

    def render(qp: QueryPart)(implicit arf: ActorRefFactory): String = qp match {
        case RawQuery(q) ⇒ q
        case FieldValue(f, v) ⇒ f.fold(valueEsc(v))(fn ⇒ s"$fn:${valueEsc(v)}")
        case OrQuery(parts) ⇒ parts map render mkString ("(", " OR ", ")")
        case AndQuery(parts) ⇒ parts map render mkString ("(", " AND ", ")")
        case Not(q) ⇒ s"-${render(q)}"
        case Range(f, l, u) ⇒ f.fold(s"[$l TO $u]")(fn ⇒ s"$fn:[$l TO $u]")
        case IsAnyOf(field, values) ⇒
            if (values.size > Solr.Client.maxBooleanClauses)
                throw new IllegalArgumentException(
                    s"akkasolr.solrMaxBooleanClauses=${Solr.Client.maxBooleanClauses} but given ${values.size} values")

            val fieldVals = {
                field match {
                    case Some(f) ⇒ values map (v ⇒ f + ':' + valueEsc(v))
                    case None ⇒ values map (v ⇒ valueEsc(v))
                }
            }
            fieldVals.mkString("(", " OR ", ")")
    }
}
