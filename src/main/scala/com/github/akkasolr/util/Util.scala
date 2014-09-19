package com.github.akkasolr.util

import spray.http.Uri

import akka.util.Helpers

/**
 * @author steven
 *
 */
object Util {
    def normalize(uri: String) = {
        val u = Uri(uri)
        if (u.path.reverse.startsWithSlash)
            u withPath u.path.reverse.tail.reverse
        else
            u
    }

    def actorNamer(prefix: String) = {
        (LongIterator from 0) map (i ⇒ s"$prefix${Helpers.base64(i)}")
    }
}
