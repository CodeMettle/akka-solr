/*
 * DefaultConnectionProvider.scala
 *
 * Updated: Sep 26, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr.ext

import spray.http.Uri

import com.codemettle.akkasolr.client.ClientConnection

/**
 * @author steven
 *
 */
class DefaultConnectionProvider extends ConnectionProvider {
    override def connectionActorProps(uri: Uri) = ClientConnection props uri
}
