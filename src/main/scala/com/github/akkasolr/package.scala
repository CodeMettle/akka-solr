package com.github

import spray.http.Uri

import akka.actor.ActorRefFactory

/**
 * @author steven
 *
 */
package object akkasolr {
    implicit class RichUri(val u: Uri) extends AnyVal {
        def isSsl = u.scheme == "https"
        def host = u.authority
    }

    def actorSystem(implicit arf: ActorRefFactory) = spray.util.actorSystem
}
