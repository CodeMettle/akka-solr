/*
 * SolrQueryResponseHeader.scala
 *
 * Updated: Sep 23, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.solrtypes

import java.{lang => jl}

import org.apache.solr.common.util.NamedList

import com.codemettle.akkasolr.CollectionConverters._

import akka.http.scaladsl.model.Uri

/**
 * @author steven
 *
 */
@SerialVersionUID(1L)
case class SolrQueryResponseHeader(original: NamedList[AnyRef]) {
    @transient
    lazy val status: Int = original get "status" match {
        case null => -1
        case i: jl.Integer => i.intValue()
        case _ => -1
    }

    @transient
    lazy val queryTime: Long = original get "QTime" match {
        case null => -1L
        case lo: jl.Long => lo.longValue()
        case i: jl.Integer => i.longValue()
        case _ => -1L
    }

    @transient
    lazy val params: Uri.Query = original get "params" match {
        case null => Uri.Query.Empty
        case nl: NamedList[_] => nl.asScala.foldRight(Uri.Query.Empty: Uri.Query) {
            case (e, acc) =>
                val value = e.getValue match {
                    case s: String => s
                    case o => o.toString
                }
                (e.getKey -> value) +: acc
        }
    }
}
