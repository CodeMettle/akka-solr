/*
 * TestActorInputStream.scala
 *
 * Updated: Sep 19, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.util

import java.io.ByteArrayInputStream

import org.scalatest._

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.util.ByteString
import scala.compat.Platform
import scala.concurrent.{Future, blocking}
import scala.util.Random

/**
 * @author steven
 *
 */
class TestActorInputStream(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers {
    def this() = this(ActorSystem("Test"))

    def readFully(ais: ActorInputStream) = {
        val buf = new Array[Byte](0x1000)
        val bs = ByteString.newBuilder
        var read = 0
        do {
            read = ais read buf
            if (read > 0)
                bs.putBytes(buf, 0, read)
        } while (read >= 0)

        bs.result().toArray
    }

    def randomBytes(len: Int) = {
        val b = new Array[Byte](len)
        Random nextBytes b
        b
    }

    def newStream = new ActorInputStream

    "An ActorInputStream" should "successfully eof" in {
        val ais = newStream

        ais.streamFinished()

        ais.read() should equal (-1)

        ais.close()
    }

    it should "successfully read bytes" in {
        val rand = randomBytes(1024)

        val ais = newStream

        ais enqueueBytes ByteString(rand)
        ais.streamFinished()

        val out = readFully(ais)

        out should equal (rand)

        ais.close()
    }

    it should "read bytes while bytes are still incoming" in {
        val rand = randomBytes(0x4000)

        val ais = newStream

        import scala.concurrent.ExecutionContext.Implicits.global

        Future(blocking {
            Thread.sleep(250)
            val bais = new ByteArrayInputStream(rand)
            val buf = new Array[Byte](0x2000)
            while (bais.available() > 0) {
                val toread = 0x1000 + Random.nextInt(0x1000)
                val count = bais.read(buf, 0, toread)
                val toenqueue = new Array[Byte](count)
                Platform.arraycopy(buf, 0, toenqueue, 0, count)
                ais enqueueBytes ByteString(toenqueue)
                Thread.sleep(250)
            }
            ais.streamFinished()
        })

        val out = readFully(ais)

        out should equal (rand)

        ais.close()
    }
}
