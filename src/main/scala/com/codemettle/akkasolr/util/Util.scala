/*
 * Util.scala
 *
 * Updated: Oct 10, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.util

import java.io.{BufferedWriter, OutputStreamWriter}

import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.common.{SolrInputDocument, SolrInputField}

import akka.http.scaladsl.model.{HttpCharsets, Uri}
import akka.util.{ByteString, Helpers}
import scala.collection.JavaConverters._

/**
 * @author steven
 *
 */
object Util {
    /**
     * Creates a normalized Uri
     * @param solrUrl URL string to normalize
     * @return [[Uri]] with any trailing '/' dropped
     */
    def normalize(solrUrl: String): Uri = {
        val u = Uri(solrUrl)
        val reversePath = u.path.reverse
        if (reversePath.startsWithSlash)
            u withPath reversePath.tail.reverse
        else
            u
    }

    def actorNamer(prefix: String): Iterator[String] = {
        (LongIterator from 0) map (i ⇒ s"$prefix${Helpers.base64(i)}")
    }

    def createUpdateRequest(docs: SolrInputDocument*): UpdateRequest = {
        val ur = new UpdateRequest
        ur add docs.asJavaCollection
        ur
    }

    def createQueryDeleteUpdateRequest(queries: String*): UpdateRequest = {
        val ur = new UpdateRequest
        ur setDeleteQuery queries.asJava
        ur
    }

    def createIdDeleteUpdateRequest(ids: String*): UpdateRequest = {
        val ur = new UpdateRequest
        ur deleteById ids.asJava
        ur
    }

    def updateRequestToByteString(ur: UpdateRequest): ByteString = {
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

    def createSolrInputDocs(fieldMaps: Map[String, AnyRef]*): Seq[SolrInputDocument] = {
        fieldMaps map (fieldMap ⇒ {
            val fields = fieldMap map {
                case (name, value) ⇒
                    val field = new SolrInputField(name)
                    field.setValue(value)
                    name → field
            }

            new SolrInputDocument(fields.asJava)
        })
    }
}
