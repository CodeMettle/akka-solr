/*
 * Util.scala
 *
 * Updated: Oct 3, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.util

import java.io.{BufferedWriter, OutputStreamWriter}

import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.common.{SolrInputDocument, SolrInputField}
import spray.http.{HttpCharsets, Uri}

import com.codemettle.akkasolr.Solr

import akka.util.{ByteString, Helpers}
import scala.collection.JavaConverters._

/**
 * @author steven
 *
 */
object Util {
    def normalize(solrUrl: String) = {
        val u = Uri(solrUrl)
        if (u.path.reverse.startsWithSlash)
            u withPath u.path.reverse.tail.reverse
        else
            u
    }

    def actorNamer(prefix: String) = {
        (LongIterator from 0) map (i ⇒ s"$prefix${Helpers.base64(i)}")
    }

    def createUpdateRequest(docs: SolrInputDocument*) = {
        val ur = new UpdateRequest
        ur add docs.asJavaCollection
        ur
    }

    def createQueryDeleteUpdateRequest(queries: String*) = {
        val ur = new UpdateRequest
        ur setDeleteQuery queries.asJava
        ur
    }

    def createIdDeleteUpdateRequest(ids: String*) = {
        val ur = new UpdateRequest
        ur deleteById ids.asJava
        ur
    }

    def updateRequestToByteString(ur: UpdateRequest) = {
        val bsb = ByteString.newBuilder
        val writer = new BufferedWriter(new OutputStreamWriter(bsb.asOutputStream, HttpCharsets.`UTF-8`.nioCharset))

        // https://issues.apache.org/jira/browse/SOLR-2277; dunno if this has been fixed in later SolrJ versions...

        writer append "<update>"

        ur writeXML writer

        writer append "</update>"

        writer.flush()
        writer.close()
        bsb.result()
    }

    def createSolrInputDocs(fieldMaps: Map[String, AnyRef]*) = {
        fieldMaps map (fieldMap ⇒ {
            val fields = fieldMap map {
                case (name, value) ⇒
                    val field = new SolrInputField(name)
                    field.setValue(value, 1.0f)
                    name → field
            }

            new SolrInputDocument(fields.asJava)
        })
    }
}
