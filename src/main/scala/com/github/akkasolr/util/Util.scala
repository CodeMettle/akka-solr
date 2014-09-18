package com.github.akkasolr.util

import spray.http.Uri

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
}
