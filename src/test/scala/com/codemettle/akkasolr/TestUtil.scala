package com.codemettle.akkasolr

import akka.actor.{ActorRefFactory, ActorSystem}
import akka.stream.Materializer
import akka.util.Timeout
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

/**
  * Created by steven on 6/7/2017.
  */
object TestUtil {
  case class SysMat(sys: ActorSystem, mat: Materializer)

  implicit class RichFuture[+T](val f: Future[T]) extends AnyVal {
    def await(implicit timeout: Timeout = 1.minute): T = Await.result(f, timeout.duration)
  }

  import scala.language.implicitConversions
//  implicit def sm2sys(sm: SysMat): ActorSystem = sm.sys
  implicit def sm2mat(sm: SysMat): Materializer = sm.mat
  implicit def sm2arf(sm: SysMat): ActorRefFactory = sm.sys
}
