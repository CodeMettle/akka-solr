/*
 * SolrQueryBuilder.scala
 *
 * Updated: Nov 21, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr
package querybuilder

import java.{util ⇒ ju}

import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.SolrQuery.SortClause
import org.apache.solr.common.params.{FacetParams ⇒ SolrFacetParams, GroupParams ⇒ SolrGroupParams, ShardParams,
SolrParams, StatsParams}

import com.codemettle.akkasolr.querybuilder.SolrQueryBuilder.ImmutableSolrParams
import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.{QueryPart, RawQuery}

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
case class GroupParams(field: Option[String] = None, sortsList: Vector[SortClause] = Vector.empty,
                       format: Option[String] = None, main: Option[Boolean] = None, totalCount: Option[Boolean] = None,
                       truncate: Option[Boolean] = None, limit: Option[Int] = None) {
    def withField(f: String) = if (field contains f) this else copy(field = Some(f))
    def withoutField() = if (field.isEmpty) this else copy(field = None)

    def sort(sorts: Iterable[SortClause]) = {
        val vec = sorts.toVector
        if (sortsList == vec) this else copy(sortsList = vec)
    }
    def withSort(sc: SortClause) = sortsList indexWhere (_.getItem == sc.getItem) match {
        case idx if idx < 0 ⇒ copy(sortsList = sortsList :+ sc)
        case idx ⇒ copy(sortsList = sortsList.updated(idx, sc))
    }
    def withSorts(scs: SortClause*) = (this /: scs) { case (gp, sc) ⇒ gp withSort sc }
    def withoutSort(sc: SortClause) = if (sortsList contains sc) copy(sortsList = sortsList.filterNot(_ == sc)) else this
    def withoutSorts() = if (sortsList.isEmpty) this else copy(sortsList = Vector.empty)

    def withFormat(gf: String) = if (format contains gf) this else copy(format = Some(gf))
    def withoutFormat() = if (format.isEmpty) this else copy(format = None)

    def setMain(m: Boolean) = if (main contains m) this else copy(main = Some(m))
    def setTotalCount(n: Boolean) = if (totalCount contains n) this else copy(totalCount = Some(n))
    def setTruncate(t: Boolean) = if (truncate contains t) this else copy(truncate = Some(t))

    def withLimit(lim: Int) = if (limit contains lim) this else copy(limit = Some(lim))
    def withoutLimit() = if (limit.isEmpty) this else copy(limit = None)
}

@SerialVersionUID(1L)
case class FacetParams(fields: Vector[String] = Vector.empty, limit: Option[Int] = None, minCount: Option[Int] = None,
                       prefix: Option[String] = None, pivotFieldList: Vector[String] = Vector.empty) {
    def setFields(fs: String*) = {
        val vec = fs.toVector
        if (fields == vec) this else copy(fields = vec)
    }
    def withField(f: String) = if (fields contains f) this else copy(fields = fields :+ f)
    def withFields(fs: String*) = (this /: fs) { case (fp, f) ⇒ fp withField f }
    def withoutField(f: String) = if (fields contains f) copy(fields = fields.filterNot(_ == f)) else this
    def withoutFields(fs: String*) = (this /: fs) { case (fp, f) ⇒ fp withoutField f }
    def withoutFields() = if (fields.isEmpty) this else copy(fields = Vector.empty)

    def withLimit(lim: Int) = if (limit contains lim) this else copy(limit = Some(lim))
    def withoutLimit() = if (limit.isEmpty) this else copy(limit = None)

    def withMinCount(mc: Int) = if (minCount contains mc) this else copy(minCount = Some(mc))
    def withoutMinCount() = if (minCount.isEmpty) this else copy(minCount = None)

    def withPrefix(p: String) = if (prefix contains p) this else copy(prefix = Some(p))
    def withoutPrefix() = if (prefix.isEmpty) this else copy(prefix = None)

    def pivotFields(pfs: String*) = {
        val vec = pfs.toVector
        if (vec == pivotFieldList) this else copy(pivotFieldList = vec)
    }
    def withPivotField(f: String) = if (pivotFieldList contains f) this else copy(pivotFieldList = pivotFieldList :+ f)
    def withPivotFields(fs: String*) = (this /: fs) { case (fp, f) ⇒ fp withPivotField f }
    def withoutPivotField(f: String) = if (pivotFieldList contains f) copy(pivotFieldList = pivotFieldList.filterNot(_ == f)) else this
    def withoutPivotFields(fs: String*) = (this /: fs) { case (fp, f) ⇒ fp withoutPivotField f }
    def withoutPivotFields() = if (pivotFieldList.isEmpty) this else copy(pivotFieldList = Vector.empty)
}

@SerialVersionUID(2L)
case class SolrQueryBuilder(query: QueryPart, rowsOpt: Option[Int] = None, startOpt: Option[Int] = None,
                            fieldList: Vector[String] = Vector.empty, sortsList: Vector[SortClause] = Vector.empty,
                            serverTimeAllowed: Option[Int] = None, facetParams: FacetParams = FacetParams(),
                            cursorMarkOpt: Option[String] = None, groupParams: GroupParams = GroupParams(),
                            statsFields: Vector[String] = Vector.empty, statsFacetFields: Vector[String] = Vector.empty,
                            shardList: Vector[String] = Vector.empty) {
    private def copyIfChange[T](fieldVal: T, newFieldVal: (T) ⇒ T, copyIfChange: (T) ⇒ SolrQueryBuilder) = {
        val newVal = newFieldVal(fieldVal)
        if (newVal == fieldVal) this else copyIfChange(newVal)
    }

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

    def withCursorMark(c: String) = {
        if (startOpt.isDefined)
            throw new IllegalArgumentException("'start' and 'cursorMark' options are mutually exclusive")

        copy(cursorMarkOpt = Some(c))
    }

    def withoutCursorMark() = copy(cursorMarkOpt = None)

    def beginCursor() = withCursorMark(/*CursorMarkParams.CURSOR_MARK_START*/"*")

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

    /**
     * Adds a new sort field only if there isn't already a sort on this field
     */
    def withSortIfNewField(sc: SortClause) = (sortsList find (_.getItem == sc.getItem)).fold(this withSort sc)(_ ⇒ this)

    def withSorts(scs: SortClause*) = (this /: scs) { case (sqc, sc) ⇒ sqc withSort sc }

    def withoutSortField(f: String) = copy(sortsList = sortsList filterNot (_.getItem == f))

    def withoutSort(sc: SortClause) = copy(sortsList = sortsList filterNot (_ == sc))

    def withoutSorts() = if (sortsList.isEmpty) this else copy(sortsList = Vector.empty)

    private def facetParamsChange(change: (FacetParams) ⇒ FacetParams) =
        copyIfChange[FacetParams](facetParams, change, fp ⇒ copy(facetParams = fp))

    def facets(fs: String*) = facetParamsChange(_.setFields(fs: _*))

    def withFacetField(f: String) = facetParamsChange(_ withField f)

    def withFacetFields(fs: String*) = facetParamsChange(_.withFields(fs: _*))

    def withoutFacetField(f: String) = facetParamsChange(_ withoutField f)

    def withoutFacetFields(fs: String*) = facetParamsChange(_.withoutFields(fs: _*))

    def withoutFacetFields() = facetParamsChange(_.withoutFields())

    def withFacetLimit(limit: Int) = facetParamsChange(_ withLimit limit)

    def withoutFacetLimit() = facetParamsChange(_.withoutLimit())

    def withFacetMinCount(min: Int) = facetParamsChange(_ withMinCount min)

    def withoutFacetMinCount() = facetParamsChange(_.withoutMinCount())

    def withFacetPrefix(prefix: String) = facetParamsChange(_ withPrefix prefix)

    def withoutFacetPrefix() = facetParamsChange(_.withoutPrefix())

    def facetPivot(fs: String*) = facetParamsChange(_.pivotFields(fs: _*))

    def withFacetPivotField(f: String) = facetParamsChange(_ withPivotField f)

    def withFacetPivotFields(fs: String*) = facetParamsChange(_.withPivotFields(fs: _*))

    def withoutFacetPivotField(f: String) = facetParamsChange(_ withoutPivotField f)

    def withoutFacetPivotFields(fs: String*) = facetParamsChange(_.withoutPivotFields(fs: _*))

    def withoutFacetPivotFields() = facetParamsChange(_.withoutPivotFields())

    private def groupParamsChange(change: (GroupParams) ⇒ GroupParams) =
        copyIfChange[GroupParams](groupParams, change, gp ⇒ copy(groupParams = gp))

    def withGroupField(gf: String) = groupParamsChange(_ withField gf)

    def withoutGroupField() = groupParamsChange(_.withoutField())

    def groupSort(sc: SortClause) = groupParamsChange(_ sort Iterable(sc))

    def withGroupSort(sc: SortClause) = groupParamsChange(_ withSort sc)

    def withGroupSorts(scs: SortClause*) = groupParamsChange(_.withSorts(scs: _*))

    def withoutGroupSort(sc: SortClause) = groupParamsChange(_ withoutSort sc)

    def withoutGroupSorts() = groupParamsChange(_.withoutSorts())

    def withGroupFormat(gf: String) = groupParamsChange(_ withFormat gf)

    def withoutGroupFormat() = groupParamsChange(_.withoutFormat())

    def groupInMain(tf: Boolean) = groupParamsChange(_ setMain tf)

    def groupTotalCount(tf: Boolean) = groupParamsChange(_ setTotalCount tf)

    def truncateGroupings(tf: Boolean) = groupParamsChange(_ setTruncate tf)

    def withGroupLimit(limit: Int) = groupParamsChange(_ withLimit limit)

    def withoutGroupLimit() = groupParamsChange(_.withoutLimit())

    def withStatsField(f: String) = if (statsFields.contains(f)) this else copy(statsFields = statsFields :+ f)

    def withStatsFields(fs: Seq[String]) = (this /: fs) { case (sqc, f) ⇒ sqc withStatsField f }

    def withoutStatsField(f: String) = if (statsFields.contains(f)) copy(statsFields = statsFields filterNot (_ == f)) else this

    def withoutStatsFields(fs: Seq[String]) = (this /: fs) { case (sqc, f) ⇒ sqc withoutStatsField f }

    def withStatsFacetField(f: String) = if (statsFacetFields.contains(f)) this else copy(statsFacetFields = statsFacetFields :+ f)

    def withStatsFacetFields(fs: Seq[String]) = (this /: fs) { case (sqc, f) ⇒ sqc withStatsFacetField f }

    def withoutStatsFacetField(f: String) = if (statsFacetFields.contains(f)) copy(statsFacetFields = statsFacetFields filterNot (_ == f)) else this

    def withoutStatsFacetFields(fs: Seq[String]) = (this /: fs) { case (sqc, f) ⇒ sqc withoutStatsFacetField f }

    def shards(ss: String*) = {
        val vec = ss.toVector
        if (shardList == vec) this else copy(shardList = vec)
    }

    def withShard(s: String) = if (shardList contains s) this else copy(shardList = shardList :+ s)

    def withShards(ss: String*) = (this /: ss) { case (sqb, s) ⇒ sqb withShard s }

    def withoutShard(s: String) = if (shardList contains s) copy(shardList = shardList.filterNot(_ == s)) else this

    def withoutShards(ss: String*) = (this /: ss) { case (sqb, s) ⇒ sqb withoutShard s }

    def withoutShards() = if (shardList.isEmpty) this else copy(shardList = Vector.empty)

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
 *
     * @return an [[ImmutableSolrParams]] representing the state of the builder
     */
    def toParams(implicit arf: ActorRefFactory): ImmutableSolrParams = {
        val solrQuery = new SolrQuery(query.render)

        rowsOpt foreach (r ⇒ solrQuery setRows r)
        startOpt foreach (s ⇒ solrQuery setStart s)
        cursorMarkOpt foreach (c ⇒ solrQuery.set(/*CursorMarkParams.CURSOR_MARK_PARAM*/"cursorMark", c))
        solrQuery setFields (fieldList.toSeq: _*)
        sortsList foreach (s ⇒ solrQuery addSort s)
        if (facetParams.fields.nonEmpty)
            solrQuery.addFacetField(facetParams.fields.toSeq: _*)
        serverTimeAllowed foreach (ms ⇒ solrQuery setTimeAllowed ms)
        facetParams.limit.foreach(l ⇒ solrQuery.setFacetLimit(l))
        facetParams.minCount.foreach(m ⇒ solrQuery.setFacetMinCount(m))
        facetParams.prefix.foreach(p ⇒ solrQuery.setFacetPrefix(p))
        if (facetParams.pivotFieldList.nonEmpty)
            solrQuery.addFacetPivotField(facetParams.pivotFieldList.toSeq: _*)

        groupParams.field foreach { f ⇒
            solrQuery.set(SolrGroupParams.GROUP, true)
            solrQuery.set(SolrGroupParams.GROUP_FIELD, f)
        }

        if (groupParams.sortsList.nonEmpty) {
            def gSortArgs = for {
                sc ← groupParams.sortsList
            } yield s"${sc.getItem} ${sc.getOrder}"

            solrQuery.add(SolrGroupParams.GROUP_SORT, gSortArgs mkString ",")
        }
        groupParams.format.foreach(f ⇒ solrQuery.set(SolrGroupParams.GROUP_FORMAT, f))
        groupParams.main.foreach(m ⇒ solrQuery.set(SolrGroupParams.GROUP_MAIN, m))
        groupParams.totalCount.foreach(n ⇒ solrQuery.set(SolrGroupParams.GROUP_TOTAL_COUNT, n))
        groupParams.limit.foreach(n ⇒ solrQuery.set(SolrGroupParams.GROUP_LIMIT, n))
        groupParams.truncate.foreach(t ⇒ solrQuery.set(SolrGroupParams.GROUP_TRUNCATE, t))

        if (shardList.nonEmpty)
            solrQuery.set(ShardParams.SHARDS, shardList mkString ",")

        if (statsFields.nonEmpty)
            solrQuery.set(StatsParams.STATS, true)
        statsFields foreach(f => {
            solrQuery.add(StatsParams.STATS_FIELD, f)
        })
        statsFacetFields foreach(f => {
            solrQuery.add(StatsParams.STATS_FACET, f)
        })

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

    private val sortRe = "(.*) (.*)".r

    def fromSolrQuery(params: SolrQuery) = {
        def rows = Option(params.getRows) map (_.intValue())
        def start = Option(params.getStart) map (_.intValue())
        def cursorMark = Option(params.get(/*CursorMarkParams.CURSOR_MARK_PARAM*/"cursorMark"))
        def fields = Option(params.getFields) map (_.split("\\s*,\\s*").toVector) getOrElse Vector.empty
        def sorts = params.getSorts.asScala.toVector
        def facetFields = Option(params.getFacetFields) map (_.toVector) getOrElse Vector.empty
        def exeTime = Option(params.getTimeAllowed) map (_.intValue())
        def facetLimit = Option(params.get(SolrFacetParams.FACET_LIMIT)) map (_.toInt)
        def facetMinCount = Option(params.get(SolrFacetParams.FACET_MINCOUNT)) map (_.toInt)
        def facetPrefix = Option(params.get(SolrFacetParams.FACET_PREFIX))
        def facetPivotFields = Option(params.get(SolrFacetParams.FACET_PIVOT)) map { str =>
            str.split(",").toVector
        } getOrElse Vector.empty

        def groupField = Option(params.get(SolrGroupParams.GROUP_FIELD))
        def groupSorts = Option(params.get(SolrGroupParams.GROUP_SORT)) map { str =>
            str.split(",").toVector collect {
                case sortRe(i, o) ⇒ SortClause.create(i, o)
            }
        } getOrElse Vector.empty
        def groupFormat = Option(params.get(SolrGroupParams.GROUP_FORMAT))
        def groupMain = Option(params.get(SolrGroupParams.GROUP_MAIN)) map (_.toBoolean)
        def groupTotalCount = Option(params.get(SolrGroupParams.GROUP_TOTAL_COUNT)) map (_.toBoolean)
        def groupTruncate = Option(params.get(SolrGroupParams.GROUP_TRUNCATE)) map (_.toBoolean)
        def groupLimit = Option(params.get(SolrGroupParams.GROUP_LIMIT)) map (_.toInt)

        def statsFields = Option(params.getParams(StatsParams.STATS_FIELD)) map (_.toVector) getOrElse Vector.empty
        def statsFacetFields = Option(params.getParams(StatsParams.STATS_FACET)) map (_.toVector) getOrElse Vector.empty

        def shards = Option(params.get(ShardParams.SHARDS)).map(_.split(",").toVector) getOrElse Vector.empty

        SolrQueryBuilder(RawQuery(params.getQuery), rows, start, fields, sorts, exeTime, FacetParams(facetFields, facetLimit,
            facetMinCount, facetPrefix, facetPivotFields), cursorMark, GroupParams(groupField, groupSorts, groupFormat, groupMain,
            groupTotalCount, groupTruncate, groupLimit), statsFields, statsFacetFields, shards)
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
                case vec if vec.nonEmpty ⇒ Some(vec.head)
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
