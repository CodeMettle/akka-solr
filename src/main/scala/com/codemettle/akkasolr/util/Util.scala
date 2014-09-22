/*
 * Util.scala
 *
 * Updated: Sep 22, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.util

import org.apache.solr.common.params.SolrParams
import spray.http.Uri

import akka.util.Helpers

/**
 * @author steven
 *
 */
object Util {
    def normalize(uri: String) = {
        val u = Uri(uri)
        if (u.path.reverse.startsWithSlash)
            u withPath u.path.reverse.tail.reverse
        else
            u
    }

    def actorNamer(prefix: String) = {
        (LongIterator from 0) map (i ⇒ s"$prefix${Helpers.base64(i)}")
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
}
