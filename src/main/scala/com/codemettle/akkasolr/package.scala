/*
 * package.scala
 *
 * Updated: Oct 3, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle

import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.params.{SolrParams, CommonParams}
import org.apache.solr.common.util.NamedList
import spray.http.Uri

import akka.actor.ActorRefFactory
import scala.concurrent.duration.Duration

/**
 * @author steven
 *
 */
package object akkasolr {
    implicit class RichUri(val u: Uri) extends AnyVal {
        def isSsl = u.scheme == "https"
        def host = u.authority
    }

    implicit class SolrUri(val baseUri: Uri) extends AnyVal {
        def selectUri = baseUri withPath baseUri.path / "select"
        def updateUri = baseUri withPath baseUri.path / "update"
        def pingUri = baseUri withPath baseUri.path ++ Uri.Path(CommonParams.PING_HANDLER)
    }

    implicit class RichSolrParams(val sp: SolrParams) extends AnyVal {
        def toQuery = {
            // Duplicate behavior of org.apache.solr.client.solrj.util.ClientUtils.toQueryString
            def paramToKVs(paramName: String): Seq[(String, String)] = sp getParams paramName match {
                case null ⇒ Seq(paramName → Uri.Query.EmptyValue)
                case arr if arr.length == 0 ⇒ Nil
                case values ⇒ values map {
                    case null ⇒ paramName → Uri.Query.EmptyValue
                    case v ⇒ paramName → v
                }
            }

            import scala.collection.JavaConverters._

            Uri.Query((sp.getParameterNamesIterator.asScala flatMap paramToKVs).toSeq: _*)
        }
    }

    import scala.language.implicitConversions
    implicit def nlToQueryResp(nl: NamedList[AnyRef]) = new QueryResponse(nl, null)

    def actorSystem(implicit arf: ActorRefFactory) = spray.util.actorSystem

    // add this for 2.10 compilation
    implicit class DurationWithCoarsest(val d: Duration) extends AnyVal {
        def toCoarsest = d
    }

    // add this for 2.10 compilation
    implicit class OptionWithContains[T](val o: Option[T]) extends AnyVal {
        def contains(t: T) = o exists (_ == t)
    }
}
