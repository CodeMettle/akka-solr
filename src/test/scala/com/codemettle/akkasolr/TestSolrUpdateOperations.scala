/*
 * TestSolrUpdateOperations.scala
 *
 * Updated: Oct 14, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr

import org.apache.solr.client.solrj.request.AbstractUpdateRequest.ACTION
import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.common.params.UpdateParams
import org.scalatest._

import com.codemettle.akkasolr.CollectionConverters._

import akka.actor.ActorSystem
import akka.testkit.TestKit
import scala.concurrent.duration._

/**
 * @author steven
 *
 */
class TestSolrUpdateOperations(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers {
    def this() = this(ActorSystem("Test"))

    "Solr.Commit" should "convert to/from SolrJ UpdateRequest" in {
        val c1 = Solr.Commit(waitForSearcher = false, softCommit = true)

        val ur1 = c1.solrJUpdateRequest

        ur1.getAction should equal (ACTION.COMMIT)

        ur1.getParams.getBool(UpdateParams.WAIT_SEARCHER) should equal (false)
        ur1.getParams.getBool(UpdateParams.SOFT_COMMIT) should equal (true)

        val update = Solr.Update(ur1)

        update.addDocs shouldBe empty
        update.deleteIds shouldBe empty
        update.deleteQueries shouldBe empty
        update.updateOptions.commit should equal (true)
    }

    "Solr.Optimize" should "convert to/from SolrJ UpdateRequest" in {
        val o1 = Solr.Optimize(waitForSearcher = false, 2)

        val ur1 = o1.solrJUpdateRequest

        ur1.getAction should equal (ACTION.OPTIMIZE)
        ur1.getParams.getBool(UpdateParams.WAIT_SEARCHER) should equal (false)
        ur1.getParams.getInt(UpdateParams.MAX_OPTIMIZE_SEGMENTS) should equal (2)

        the[Solr.InvalidRequest] thrownBy Solr.Update(ur1) should have message "Solr.Update doesn't currently support optimizing; use Solr.Optimize"

        val opt = Solr.Optimize(ur1)
        opt.waitForSearcher should equal (false)
        opt.maxSegments should equal (2)

        val suo1 = Solr.SolrUpdateOperation(ur1)
        suo1 shouldBe a[Solr.Optimize]
        suo1 should equal (opt)
    }

    it should "throw exception if non-optimize UpdateRequest passed in" in {
        val ur1 = new UpdateRequest()

        the[Solr.InvalidRequest] thrownBy Solr.Optimize(ur1) should have message "Request isn't an optimize request"

        ur1.setAction(ACTION.COMMIT, true, true)

        the[Solr.InvalidRequest] thrownBy Solr.Optimize(ur1) should have message "Request isn't an optimize request"

        ur1.setParams(null)

        ur1.setAction(ACTION.OPTIMIZE, true, false, 2)

        val opt = Solr.Optimize(ur1)
        opt.waitForSearcher should equal (false)
        opt.maxSegments should equal (2)
    }

    "Solr.Rollback" should "convert to/from SolrJ UpdateRequest" in {
        val r1 = Solr.Rollback()

        val ur1 = r1.solrJUpdateRequest

        ur1.getParams.getBool(UpdateParams.ROLLBACK) should equal (true)

        the[Solr.InvalidRequest] thrownBy Solr.Update(ur1) should have message "Solr.Update doesn't currently support rollback; use Solr.Rollback"

        val rb = Solr.Rollback(ur1)
        rb shouldBe a[Solr.Rollback]

        val suo1 = Solr.SolrUpdateOperation(ur1)
        suo1 shouldBe a[Solr.Rollback]
        suo1 should equal (rb)
    }

    it should "throw exception if non-rollback UpdateRequest passed in" in {
        val ur1 = new UpdateRequest()

        the[Solr.InvalidRequest] thrownBy Solr.Rollback(ur1) should have message "Request isn't a rollback request"

        ur1.rollback()

        val rb = Solr.Rollback(ur1)
        rb shouldBe a[Solr.Rollback]
    }

    "Solr.Update" should "convert to/from SolrJ UpdateRequest" in {
        import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.Methods._

        def randDocs = (0 until 5) map (i => Map[String, AnyRef]("id" -> (i: java.lang.Integer)))

        val upd = Solr.Update AddDocs (randDocs: _*) deleteByQuery OR(defaultField() := "1", field("id") := 2) deleteById "3" deleteById "4"

        val ur1 = upd.solrJUpdateRequest

        ur1.getDeleteQuery should have size 1
        ur1.getDeleteQuery.get(0) should equal ("(1 OR id:2)")
        ur1.getDeleteById should have size 2
        ur1.getDeleteById.get(0) should equal ("3")
        ur1.getDeleteById.get(1) should equal ("4")
        ur1.getDocuments should have size 5
        ur1.getDocuments.asScala.zipWithIndex foreach {case (doc, i) => doc.getFieldValue("id") should equal (i)}

        Solr.Update(ur1) should equal (upd)

        val updCW = upd commitWithin 5.seconds
        val ur2 = updCW.solrJUpdateRequest
        ur2.getDocumentsMap.values.asScala foreach (_.get(UpdateRequest.COMMIT_WITHIN) should equal (5000))
        ur2.getCommitWithin should equal (5000)

        Solr.Update(ur2) should equal (updCW)

        val updOW = upd overwrite false
        val ur3 = updOW.solrJUpdateRequest
        ur3.getDocumentsMap.values.asScala foreach (_.get(UpdateRequest.OVERWRITE) should equal (false))

        Solr.Update(ur3) should equal (updOW)

        val updCWOW = updCW overwrite false
        val ur4 = updCWOW.solrJUpdateRequest
        ur4.getDocumentsMap.values.asScala foreach (d => {
            d.get(UpdateRequest.COMMIT_WITHIN) should equal (5000)
            d.get(UpdateRequest.OVERWRITE) should equal (false)
        })
        ur4.getCommitWithin should equal (5000)

        Solr.Update(ur4) should equal (updCWOW)

        val updCmt = upd commit true
        val ur5 = updCmt.solrJUpdateRequest
        ur5.getAction should equal (ACTION.COMMIT)

        Solr.Update(ur5).updateOptions.commit should equal (true)
        Solr.Update(ur5) should equal (updCmt)

        val updCWOWCmt = updCWOW commit true
        val ur6 = updCWOWCmt.solrJUpdateRequest
        ur6.getDocumentsMap.values.asScala foreach (d => {
            d.get(UpdateRequest.COMMIT_WITHIN) should equal (5000)
            d.get(UpdateRequest.OVERWRITE) should equal (false)
        })
        ur6.getCommitWithin should equal (5000)
        ur6.getAction should equal (ACTION.COMMIT)

        Solr.Update(ur6) should equal (updCWOWCmt)

        val suo1 = Solr.SolrUpdateOperation(ur6)
        suo1 shouldBe a[Solr.Update]
        suo1 should equal (updCWOWCmt)
    }
}
