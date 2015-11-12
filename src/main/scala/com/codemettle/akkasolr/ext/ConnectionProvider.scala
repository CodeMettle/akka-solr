/*
 * ConnectionProvider.scala
 *
 * Updated: Oct 3, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.ext

import spray.http.Uri

import akka.actor.{ExtendedActorSystem, Props}

/**
 * @author steven
 *
 */
trait ConnectionProvider {
    def connectionActorProps(uri: Uri, username: Option[String], password: Option[String], system: ExtendedActorSystem): Props
}
