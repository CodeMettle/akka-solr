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

import akka.actor.{Props, ExtendedActorSystem}

/**
 * @author steven
 *
 */
class DefaultConnectionProvider extends ConnectionProvider {
    override def connectionActorProps(uri: Uri, system: ExtendedActorSystem) = ClientConnection props uri
}
