/*
 * ConnectionProvider.scala
 *
 * Updated: Sep 26, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.ext

import spray.http.Uri

import akka.actor.{Actor, Props}

/**
 * @author steven
 *
 */
trait ConnectionProvider {
    def connectionActorProps(uri: Uri): Props
}
