package com.github.akkasolr.util

import java.io.{ByteArrayOutputStream, InputStream}

import com.github.akkasolr.util.ActorInputStream._

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import scala.compat.Platform
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * @author steven
 *
 */
object ActorInputStream {
    private case class EnqueueBytes(bytes: Array[Byte])
    private case object TriggerStreamComplete
    private case class DequeueBytes(max: Int)
    private case class DequeuedBytes(bytes: Option[Array[Byte]])

    private class ByteBuffer extends Actor with Stash {
        private val baos = new ByteArrayOutputStream
        private var finished = false

        def receive = {
            case TriggerStreamComplete ⇒
                finished = true
                unstashAll()

            case EnqueueBytes(bytes) ⇒
                baos write bytes
                unstashAll()

            case DequeueBytes(max) ⇒
                if (baos.size() == 0 && finished)
                    sender() ! DequeuedBytes(None)
                else if (baos.size() == 0)
                    stash()
                else {
                    val bytes = baos.toByteArray
                    baos.reset()

                    if (bytes.length <= max)
                        sender() ! DequeuedBytes(Some(bytes))
                    else {
                        val bytesToSend = new Array[Byte](max)
                        Platform.arraycopy(bytes, 0, bytesToSend, 0, max)

                        sender() ! DequeuedBytes(Some(bytesToSend))

                        baos.write(bytes, max, bytes.length - max)
                    }
                }
        }
    }
}

class ActorInputStream(arf: ActorRefFactory) extends InputStream {
    private implicit val timeout = Timeout(90.seconds)
    private val buffer = arf.actorOf(Props[ByteBuffer])

    def streamFinished() = buffer ! TriggerStreamComplete

    def enqueueBytes(bytes: Array[Byte]) = buffer ! EnqueueBytes(bytes)

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
                case DequeuedBytes(None) ⇒ -1
                case DequeuedBytes(Some(bytes)) ⇒
                    Platform.arraycopy(bytes, 0, b, off, bytes.length)
                    bytes.length
            }
        }
    }
}
