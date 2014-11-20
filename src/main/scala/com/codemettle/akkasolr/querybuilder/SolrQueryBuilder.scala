/*
 * SolrQueryBuilder.scala
 *
 * Updated: Oct 9, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr
package querybuilder

import java.{util => ju}

import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.SolrQuery.SortClause
import org.apache.solr.common.params.{FacetParams, SolrParams}

import com.codemettle.akkasolr.querybuilder.SolrQueryBuilder.ImmutableSolrParams
import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.{RawQuery, QueryPart}

import akka.actor.ActorRefFactory
import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * An (incomplete) immutable builder for Solr queries. Currently only has common
 * [[org.apache.solr.client.solrj.SolrQuery]] shortcuts, but more can be added easily as the need arises.
 *
 * === Sample Usage ===
 *
 * {{{
 *     import com.codemettle.akkasolr.client.SolrQueryBuilder.FieldStrToSort
 *
 *     val b = SolrQueryBuilder("*") rows 21 fields "field" sortBy "myfield".desc
 *     val b2 = b facets "facetfield"
 *     query(b.toParams)
 *     query(b2.toParams)
 * }}}
 *
 * @author steven
 */
@SerialVersionUID(1L)
case class SolrQueryBuilder(query: QueryPart, rowsOpt: Option[Int] = None, startOpt: Option[Int] = None,
                            fieldList: Vector[String] = Vector.empty, sortsList: Vector[SortClause] = Vector.empty,
                            facetFields: Vector[String] = Vector.empty, serverTimeAllowed: Option[Int] = None,
                            facetLimit: Option[Int] = None, facetMinCount: Option[Int] = None,
                            facetPrefix: Option[String] = None, cursorMarkOpt: Option[String] = None) {
    /* ** builder shortcuts ***/

    def withQuery(q: String) = copy(query = RawQuery(q))

    def withQuery(qp: QueryPart)(implicit arf: ActorRefFactory) = copy(query = qp)

    def rows(r: Int) = copy(rowsOpt = Some(r))

    def withoutRows() = copy(rowsOpt = None)

    def start(s: Int) = {
        if (cursorMarkOpt.isDefined)
            throw new IllegalArgumentException("'start' and 'cursorMark' options are mutually exclusive")

        copy(startOpt = Some(s))
    }

    def withoutStart() = copy(startOpt = None)

    def cursorMark(c: String) = {
        if (startOpt.isDefined)
            throw new IllegalArgumentException("'start' and 'cursorMark' options are mutually exclusive")

        copy(cursorMarkOpt = Some(c))
    }

    def withoutCursorMark() = copy(cursorMarkOpt = None)

    def beginCursor() = cursorMark(/*CursorMarkParams.CURSOR_MARK_START*/"*")

    def fields(fs: String*) = copy(fieldList = fs.toVector)

    def withField(f: String) = if (fieldList.contains(f)) this else copy(fieldList = fieldList :+ f)

    def withFields(fs: String*) = (this /: fs) { case (sqc, f) ⇒ sqc withField f }

    def withoutField(f: String) = if (fieldList.contains(f)) copy(fieldList = fieldList filterNot (_ == f)) else this

    def withoutFields(fs: String*) = (this /: fs) { case (sqc, f) ⇒ sqc withoutField f}

    def withoutFields() = if (fieldList.isEmpty) this else copy(fieldList = Vector.empty)

    def sortBy(sc: SortClause) = copy(sortsList = Vector(sc))

    def sortBy(scs: SortClause*) = copy(sortsList = scs.toVector)

    def withSort(sc: SortClause) = sortsList indexWhere (_.getItem == sc.getItem) match {
        case idx if idx < 0 ⇒ copy(sortsList = sortsList :+ sc)
        case idx ⇒ copy(sortsList = sortsList updated (idx, sc))
    }

    def withSorts(scs: SortClause*) = (this /: scs) { case (sqc, sc) ⇒ sqc withSort sc }

    def withoutSortField(f: String) = copy(sortsList = sortsList filterNot (_.getItem == f))

    def withoutSort(sc: SortClause) = copy(sortsList = sortsList filterNot (_ == sc))

    def withoutSorts() = if (sortsList.isEmpty) this else copy(sortsList = Vector.empty)

    def facets(fs: String*) = copy(facetFields = fs.toVector)

    def withFacetField(f: String) = if (facetFields.contains(f)) this else copy(facetFields = facetFields :+ f)

    def withFacetFields(fs: String*) = (this /: fs) { case (sqc, f) ⇒ sqc withFacetField f }

    def withoutFacetField(f: String) = if (facetFields.contains(f)) copy(facetFields = facetFields filterNot (_ == f)) else this

    def withoutFacetFields(fs: String*) = (this /: fs) { case (sqc, f) ⇒ sqc withoutFacetField f }

    def withoutFacetFields() = if (facetFields.isEmpty) this else copy(facetFields = Vector.empty)

    def withFacetLimit(limit: Int) = if (facetLimit contains limit) this else copy(facetLimit = Some(limit))

    def withoutFacetLimit() = if (facetLimit.isEmpty) this else copy(facetLimit = None)

    def withFacetMinCount(min: Int) = if (facetMinCount contains min) this else copy(facetMinCount = Some(min))

    def withoutFacetMinCount() = if (facetMinCount.isEmpty) this else copy(facetMinCount = None)

    def withFacetPrefix(prefix: String) = if (facetPrefix contains prefix) this else copy(facetPrefix = Some(prefix))

    def withoutFacetPrefix() = if (facetPrefix.isEmpty) this else copy(facetPrefix = None)

    def allowedExecutionTime(millis: Int) = copy(serverTimeAllowed = Some(millis))

    def allowedExecutionTime(duration: FiniteDuration) = duration.toMillis match {
        case ms if ms > Int.MaxValue ⇒ throw new IllegalArgumentException("Execution time too large")
        case ms ⇒ copy(serverTimeAllowed = Some(ms.toInt))
    }

    def allowedExecutionTime(duration: Duration): SolrQueryBuilder = duration match {
        case fd: FiniteDuration ⇒ allowedExecutionTime(fd)
        case _ ⇒ withoutAllowedExecutionTime()
    }

    def withoutAllowedExecutionTime() = copy(serverTimeAllowed = None)

    /*** solrquery creation ***/

    /**
     * Create a [[SolrParams]] object that can be used for Solr queries
     * @return an [[ImmutableSolrParams]] representing the state of the builder
     */
    def toParams(implicit arf: ActorRefFactory): ImmutableSolrParams = {
        val solrQuery = new SolrQuery(query.render)

        rowsOpt foreach (r ⇒ solrQuery setRows r)
        startOpt foreach (s ⇒ solrQuery setStart s)
        cursorMarkOpt foreach (c ⇒ solrQuery.set(/*CursorMarkParams.CURSOR_MARK_PARAM*/"cursorMark", c))
        solrQuery setFields (fieldList.toSeq: _*)
        sortsList foreach (s ⇒ solrQuery addSort s)
        if (facetFields.nonEmpty)
            solrQuery addFacetField (facetFields.toSeq: _*)
        serverTimeAllowed foreach (ms ⇒ solrQuery setTimeAllowed ms)
        facetLimit foreach (l ⇒ solrQuery.setFacetLimit(l))
        facetMinCount foreach (m ⇒ solrQuery.setFacetMinCount(m))
        facetPrefix foreach (p ⇒ solrQuery.setFacetPrefix(p))

        ImmutableSolrParams(solrQuery)
    }
}

object SolrQueryBuilder {
    implicit class FieldStrToSort(val f: String) extends AnyVal {
        def ascending = new SortClause(f, SolrQuery.ORDER.asc)
        def asc = ascending
        def descending = new SortClause(f, SolrQuery.ORDER.desc)
        def desc = descending
    }

    def fromSolrQuery(params: SolrQuery) = {
        def rows = Option(params.getRows) map (_.intValue())
        def start = Option(params.getStart) map (_.intValue())
        def cursorMark = Option(params.get(/*CursorMarkParams.CURSOR_MARK_PARAM*/"cursorMark"))
        def fields = Option(params.getFields) map (_.split("\\s*,\\s*").toVector) getOrElse Vector.empty
        def sorts = params.getSorts.asScala.toVector
        def facetFields = Option(params.getFacetFields) map (_.toVector) getOrElse Vector.empty
        def exeTime = Option(params.getTimeAllowed) map (_.intValue())
        def facetLimit = Option(params.get(FacetParams.FACET_LIMIT)) map (_.toInt)
        def facetMinCount = Option(params.get(FacetParams.FACET_MINCOUNT)) map (_.toInt)
        def facetPrefix = Option(params.get(FacetParams.FACET_PREFIX))

        SolrQueryBuilder(RawQuery(params.getQuery), rows, start, fields, sorts, facetFields, exeTime, facetLimit,
            facetMinCount, facetPrefix, cursorMark)
    }

    /*
     * Didn't use Solr's immutable MultiMapSolrParams because it'd be a lot of converting collections back and forth,
     * and it's missing a SerialVersionUID (at least in 4.5)
     */
    /**
     * An immutable implementation of [[SolrParams]]; like [[org.apache.solr.common.params.MultiMapSolrParams]] but
     * Scala-ish
     */
    @SerialVersionUID(1L)
    case class ImmutableSolrParams(params: HashMap[String, Vector[String]] = HashMap.empty) extends SolrParams {
        override def get(param: String): String = {
            (params get param flatMap {
                case null ⇒ None
                case vec if vec.size > 0 ⇒ Some(vec(0))
                case _ ⇒ None
            }).orNull
        }

        override def getParameterNamesIterator: ju.Iterator[String] = params.keys.iterator.asJava

        override def getParams(param: String): Array[String] = (params get param map (_.toArray)).orNull
    }

    object ImmutableSolrParams {
        def apply(params: SolrParams): ImmutableSolrParams = new ImmutableSolrParams(
            (HashMap.empty[String, Vector[String]] /: params.getParameterNamesIterator.asScala) {
                case (acc, param) ⇒ acc + (param → params.getParams(param).toVector)
            })
    }
}
