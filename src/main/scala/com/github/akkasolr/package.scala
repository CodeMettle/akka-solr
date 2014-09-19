package com.github

import org.apache.solr.common.params.CommonParams
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

    implicit class SolrUri(val baseUri: Uri) extends AnyVal {
        def selectUri = baseUri withPath baseUri.path / "select"
        def updateUri = baseUri withPath baseUri.path / "update"
        def pingUri = baseUri withPath baseUri.path ++ Uri.Path(CommonParams.PING_HANDLER)
    }

    def actorSystem(implicit arf: ActorRefFactory) = spray.util.actorSystem
}
