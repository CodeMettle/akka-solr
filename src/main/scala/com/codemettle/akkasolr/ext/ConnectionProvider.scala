/*
 * ConnectionProvider.scala
 *
 * Updated: Oct 3, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.ext

import akka.actor.{ExtendedActorSystem, Props}
import akka.http.scaladsl.model.Uri
import akka.stream.Materializer

/**
 * @author steven
 *
 */
trait ConnectionProvider {
    def connectionActorProps(uri: Uri, username: Option[String], password: Option[String], system: ExtendedActorSystem)
                            (implicit mat: Materializer): Props
}
