/*
 * package.scala
 *
 * Updated: Oct 13, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle

import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.params.{CommonParams, SolrParams}
import org.apache.solr.common.util.NamedList

import akka.actor.{ActorContext, ActorRefFactory, ActorSystem}
import akka.http.scaladsl.model.Uri

/**
 * @author steven
 *
 */
package object akkasolr {
    implicit class RichUri(val u: Uri) extends AnyVal {
        def isSsl: Boolean = u.scheme == "https"
        def host: Uri.Authority = u.authority
    }

    implicit class SolrUri(val baseUri: Uri) extends AnyVal {
        def selectUri: Uri = baseUri withPath baseUri.path / "select"
        def updateUri: Uri = baseUri withPath baseUri.path / "update"
        def pingUri: Uri = baseUri withPath baseUri.path ++ Uri.Path(CommonParams.PING_HANDLER)
    }

    implicit class RichSolrParams(val sp: SolrParams) extends AnyVal {
        def toQuery: Uri.Query = {
            // Duplicate behavior of org.apache.solr.client.solrj.util.ClientUtils.toQueryString
            def paramToKVs(paramName: String): Seq[(String, String)] = sp getParams paramName match {
                case null => Seq(paramName -> Uri.Query.EmptyValue)
                case arr if arr.isEmpty => Nil
                case values => values.toIndexedSeq map {
                    case null => paramName -> Uri.Query.EmptyValue
                    case v => paramName -> v
                }
            }

            import CollectionConverters._

            Uri.Query((sp.getParameterNamesIterator.asScala flatMap paramToKVs).toSeq: _*)
        }
    }

    import scala.language.implicitConversions
    implicit def nlToQueryResp(nl: NamedList[AnyRef]): QueryResponse = new QueryResponse(nl, null)

    def actorSystem(implicit arf: ActorRefFactory): ActorSystem = arf match {
        case as: ActorSystem => as
        case ac: ActorContext => ac.system
        case _ => sys.error(s"Unsupported ActorRefFactory: $arf")
    }
}
