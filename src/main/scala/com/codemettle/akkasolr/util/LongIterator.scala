/*
 * LongIterator.scala
 *
 * Updated: Sep 19, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.util

import scala.collection.{AbstractIterator, Iterator}

/**
 * Copied from scala's Iterator
 *
 * @author Martin Odersky
 * @author Matthias Zenger
 *
 */
object LongIterator {
    def from(start: Long): Iterator[Long] = from(start, 1)

    def from(start: Long, step: Long): Iterator[Long] = new AbstractIterator[Long] {
        private var i = start
        def hasNext: Boolean = true
        def next(): Long = { val result = i; i += step; result }
    }
}
