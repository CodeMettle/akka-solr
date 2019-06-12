/*
 * AkkaSolrDocument.scala
 *
 * Updated: Sep 23, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.solrtypes

import java.{lang => jl}

import org.apache.solr.common.SolrDocument

import com.codemettle.akkasolr.CollectionConverters._
import com.codemettle.akkasolr.solrtypes.AkkaSolrDocument.FieldAccessor

import scala.reflect.ClassTag

/**
 * @author steven
 *
 */
@SerialVersionUID(1L)
case class AkkaSolrDocument(original: SolrDocument) {
    @transient
    lazy val asMap: Map[String, AnyRef] = original.entrySet().asScala.foldLeft(Map.empty[String, AnyRef]) {
        case (acc, e) => acc + (e.getKey -> e.getValue)
    }

    @transient
    lazy val documentVersion: Long = original get "_version_" match {
        case lo: jl.Long => lo.longValue()
        case _ => -1L
    }

    def field(name: String) = FieldAccessor(name, original get name)
}

object AkkaSolrDocument {
    @SerialVersionUID(1L)
    case class FieldAccessor(fieldName: String, fieldValue: AnyRef) {
        def asUnsafe[T : ClassTag]: T = fieldValue match {
            case null => null.asInstanceOf[T]
            case t: T => t
            case o => throw new ClassCastException(s"Field $fieldName is a ${o.getClass.getSimpleName}, " +
                s"not a ${implicitly[ClassTag[T]].runtimeClass.getSimpleName}")
        }

        def as[T : ClassTag]: Option[T] = fieldValue match {
            case null => None
            case t: T => Some(t)
            case _ => None
        }
    }
}
