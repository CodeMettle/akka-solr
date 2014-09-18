package com.github

import spray.http.Uri

import akka.actor.{ExtendedActorSystem, ActorContext, ActorRefFactory}
import scala.annotation.tailrec

/**
 * @author steven
 *
 */
package object akkasolr {
    implicit class RichUri(val u: Uri) extends AnyVal {
        def isSsl = u.scheme == "https"
        def host = u.authority
    }

    @tailrec
    def actorSystem(implicit arf: ActorRefFactory): ExtendedActorSystem = arf match {
        case x: ActorContext ⇒ actorSystem(x.system)
        case x: ExtendedActorSystem ⇒ x
        case _ ⇒ throw new IllegalArgumentException(s"Unsupported ActorRefFactory implementation: $arf")
    }
}
