/*
 * ActorInputStream.scala
 *
 * Updated: Sep 19, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr
package util

import java.io.InputStream

import com.codemettle.akkasolr.util.ActorInputStream._

import akka.actor._
import akka.pattern._
import akka.util.{ByteString, Timeout}
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * @author steven
 *
 */
object ActorInputStream {
    private case class EnqueueBytes(bytes: ByteString)
    private case object TriggerStreamComplete
    private case class DequeueBytes(max: Int)
    private case class DequeuedBytes(bytes: Option[ByteString])

    private class ByteBuffer extends Actor with Stash {
        private var byteStr = ByteString.empty
        private var finished = false

        def receive = {
            case TriggerStreamComplete =>
                finished = true
                unstashAll()

            case EnqueueBytes(bytes) =>
                byteStr ++= bytes
                unstashAll()

            case DequeueBytes(max) =>
                if (byteStr.isEmpty && finished)
                    sender() ! DequeuedBytes(None)
                else if (byteStr.isEmpty)
                    stash()
                else if (byteStr.size <= max) {
                    sender() ! DequeuedBytes(Some(byteStr))
                    byteStr = ByteString.empty
                } else {
                    val (toSend, toKeep) = byteStr splitAt max
                    sender() ! DequeuedBytes(Some(toSend))
                    byteStr = toKeep
                }
        }
    }
}

class ActorInputStream(implicit arf: ActorRefFactory) extends InputStream {
    private implicit val timeout = Timeout(90.seconds)
    private val buffer = arf.actorOf(Props[ByteBuffer])

    def streamFinished() = buffer ! TriggerStreamComplete

    def enqueueBytes(bytes: ByteString) = buffer ! EnqueueBytes(bytes)

    override def close(): Unit = buffer ! PoisonPill

    override def read(): Int = {
        val arr = new Array[Byte](1)
        if (read(arr) == -1) -1 else arr(0)
    }

    override def read(b: Array[Byte], off: Int, len: Int): Int = {
        if (b == null)
            throw new NullPointerException
        else if (off < 0 || len < 0 || len > b.length - off)
            throw new IndexOutOfBoundsException
        else if (len == 0)
            0
        else {
            Await.result(buffer ? DequeueBytes(len), Duration.Inf) match {
                case DequeuedBytes(None) => -1
                case DequeuedBytes(Some(byteStr)) =>
                    val bytes = byteStr.toArray
                    System.arraycopy(bytes, 0, b, off, bytes.length)
                    bytes.length
            }
        }
    }
}
