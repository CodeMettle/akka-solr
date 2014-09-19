package com.github.akkasolr.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.google.common.io.ByteStreams
import org.scalatest._

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.util.ByteString
import scala.compat.Platform
import scala.concurrent.{ExecutionContext, blocking, Future}
import scala.util.Random

/**
 * @author steven
 *
 */
class TestActorInputStream(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers {
    def this() = this(ActorSystem("Test"))

    def readFully(ais: ActorInputStream) = {
        val baos = new ByteArrayOutputStream
        ByteStreams.copy(ais, baos)
        baos.toByteArray
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

        import ExecutionContext.Implicits.global

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
