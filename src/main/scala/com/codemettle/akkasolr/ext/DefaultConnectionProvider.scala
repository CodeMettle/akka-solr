/*
 * DefaultConnectionProvider.scala
 *
 * Updated: Oct 3, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.ext

import spray.http.Uri

import com.codemettle.akkasolr.client.ClientConnection

import akka.actor.ExtendedActorSystem

/**
 * @author steven
 *
 */
class DefaultConnectionProvider extends ConnectionProvider {
    override def connectionActorProps(uri: Uri, username: Option[String], password: Option[String],
                                      system: ExtendedActorSystem) = ClientConnection.props(uri, username, password)
}
