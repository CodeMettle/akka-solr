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
import spray.http.Uri

import scala.collection.JavaConverters._

/**
 * @author steven
 *
 */
@SerialVersionUID(1L)
case class SolrQueryResponseHeader(original: NamedList[AnyRef]) {
    @transient
    lazy val status = original get "status" match {
        case null ⇒ -1
        case i: jl.Integer ⇒ i.intValue()
        case _ ⇒ -1
    }

    @transient
    lazy val queryTime = original get "QTime" match {
        case null ⇒ -1L
        case lo: jl.Long ⇒ lo.longValue()
        case i: jl.Integer ⇒ i.longValue()
        case _ ⇒ -1L
    }

    @transient
    lazy val params = original get "params" match {
        case null ⇒ Uri.Query.Empty
        case nl: NamedList[_] ⇒ (nl.asScala :\ (Uri.Query.Empty: Uri.Query)) {
            case (e, acc) ⇒
                val value = e.getValue match {
                    case s: String ⇒ s
                    case o ⇒ o.toString
                }
                (e.getKey → value) +: acc
        }
    }
}
