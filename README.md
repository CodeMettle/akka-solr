akka-solr
=========

A Solr5 client built on [Akka](http://akka.io) and [Spray](http://spray.io)
[![Build Status](https://travis-ci.org/CodeMettle/akka-solr.svg?branch=master)](https://travis-ci.org/CodeMettle/akka-solr)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.codemettle.akka-solr/akka-solr_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.codemettle.akka-solr/akka-solr_2.11)

The goal of akka-solr is to provide a high-performance, non-blocking, Akka-and-Scala-native interface to [Apache Solr5](http://lucene.apache.org/solr/). The initial implementation provides an interface similar to `spray-client`'s, with an Akka extension that allows requests to be sent to an actor, or an interface to request a connection actor and send requests to it. Optional builders for requests are provided, but are not required; results from Solr are returned as wrapper objects that provide easier access from Scala to SolrJ objects. Some SolrJ objects are used in the interest of maintainability.

#### Note about blocking:
In order to keep from reinventing the wheel and then maintaining said wheel, the SolrJ library is used for generating update (add/delete) requests (which could easily be replaced, and actually is buggy) and for parsing results (`XMLResponseParser`, `BinaryResponseParser`, `StreamingBinaryResponseParser`). Since the SolrJ `ResponseParser`s work from `java.io.InputSource`s, and akka-solr uses reactive/non-blocking response chunking, blocking calls were added to bridge the `InputSource` requests into Akka messages. A dedicated, runtime-configurable executor is used for all SolrJ response parsing with the `ActorInputStream` class and the `akkasolr.response-parser-dispatcher` config. Any improvements / alternate implementations are welcome. Along the same lines, SolrJ's `ZkStateReader` class is used for ZooKeeper/SolrCloud support, it has a configurable dispatcher that will be created upon first usage.


Import
------

akka-solr depends on SolrJ for request generation and response parsing, but the dependency is marked as `"provided"` so the end user is required to pull the dependency in. After cursory inspections, akka-solr is expected to work with SolrJ versions 4.5 through 4.10. Akka and spray-can are not pulled in as `"provided"`, this can be changed if feedback demands.

#### Add Dependencies:

sbt:

```scala
libraryDependencies ++= Seq(
    "com.codemettle.akka-solr" %% "akka-solr" % "2.0.0",
    "org.apache.solr" % "solr-solrj" % "5.1.0" // later versions should work but are untested
)
```

Maven:

```xml
<dependency>
    <groupId>com.codemettle.akka-solr</groupId>
    <artifactId>akka-solr</artifactId>
    <version>2.0.0</version>
</dependency>
<dependency>
    <groupId>org.apache.solr</groupId>
    <artifactId>solr-solrj</artifactId>
    <version>5.1.0</version>
</dependency>
```


Usage
-----

If you've used the `spray-can` HTTP client library, you're already familiar with akka-solr's philosophy.

Some scaladocs are provided, open a ticket for anything unclear (or submit a pull request!).

In lieu of detailed documentation, here are a list of examples (using `ask`/`?` syntax for clarity and brevity, even though the library is meant to be used from Actors with message passing).

### Bulding Request Strings:

A builder is provided as part of akka-solr, any improvements are welcome.

#### Building Solr query strings

###### Basic

```scala
import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.Methods._

val qs = rawQuery("my custom query")
val qs = defaultField() := "wantthis"
val qs = defaultField() :!= "dontwantthis"
val qs = field("myfield") := "requiredvalue"
val qs = field("myfield") isAnyOf Seq("1", "2")
val qs = field("mylong") isInRange (12345, 98765)
val qs = field("requiredField") exists()
val qs = field("illegalField") doesNotExist()
```

###### AND/OR/NOT

```scala
import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.Methods._

val qs = AND (
    field ("x") := "y",
    defaultField() :!= "3",
    NOT(field ("z") := 2),
    OR (
        rawQuery ("<my custom query>"),
        field("aa") := "2",
        field("bb") isAnyOf Seq("1", "2")
    )
)
```

###### Options

```scala
import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.Methods._

// if both lower and upper are nonempty, then the "time" field will be in the query (Some(field("time").isInRange(lo, hi)))
// if one or both are empty, then the for comprehension yields a None, and will be dropped at query render time
def buildQuery(lower: Option[Long], upper: Option[Long]) = {
  AND(
    defaultField() := "xyz",
    for (lo <- lower; hi <- upper) yield field("time") isInRange (lo, hi)
  )
}
```


#### Building queries with options

```scala
import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.Methods._
import com.codemettle.akkasolr.querybuilder.SolrQueryBuilder.FieldStrToSort

val qs: QueryPart = ???
val query = qs start 50 rows 25 facets ("a", "b") fields ("f1", "f2") sortBy "f1".desc
```

### Building Requests:

```scala
val req = Solr.Ping(action = None, options = Solr.RequestOptions(method = RequestMethods.GET, responseType = SolrResponseTypes.XML, requestTimeout = 5.seconds))

val req = Solr.Ping()
val req = Solr.Ping(Solr.Ping.Enable)

val req = Solr.Commit(waitForSearcher = false, softCommit = true)
val req = Solr.Optimize(waitForSearcher = false, maxSegments = 2)
val req = Solr.Rollback(options = Solr.RequestOptions(actorSystem).copy(method = RequestMethods.POST, responseType = SolrResponseTypes.Binary))

val req = Solr.Select(qs)
val req = Solr.Select.Streaming(qs)
val req = Solr.Select(qs).streaming
val req = Solr.Select(qs).streaming withOptions Solr.RequestOptions(actorSystem).copy(requestTimeout = 15.seconds)

val req = Solr.Update DeleteById ("id1", "id2")
val req = Solr.Update() deleteById "id1" deleteByQuery (Solr.queryStringBuilder defaultField() := "blah")

val docs: Seq[SolrInputDocument] = ???
val req = Solr.Update AddSolrDocs (docs: _*)

val docs: Seq[Map[String, AnyRef]] = ???
val req = Solr.Update AddDocs (docs: _*)

val doc: Map[String, AnyRef] = ???
val req = Solr.Update() addDoc doc
val req = Solr.Update() addDoc doc overwrite false
val req = Solr.Update() addDoc doc commit true
val req = Solr.Update() addDoc doc commitWithin 42.seconds
```

### Sending Requests:

A Connection actor is requested with `Solr.Client.clientTo()`. The connection accepts `Solr.SolrOperation` messages. `Solr.Client.manager` can also accept `Solr.Request` objects which will create connections as needed.

```scala
val req: Solr.SolrOperation = ???
val resp: Future[SolrQueryResponse] =
    (Solr.Client.manager ? Solr.Request("http://mysolrserver:8983/solr/core1", req)).mapTo[SolrQueryResponse]
```

Actor:
```scala
Solr.Client.clientTo("http://mysolrserver")
def receive = {
    case Solr.SolrConnection("http://mysolrserver", connection) => // sender() is the same actor as `connection`
}
```

Future:
```scala
val connF: Future[ActorRef] = Solr.Client.clientFutureTo("http://mysolrserver")
```

Connection actor accepts requests and sends back `SolrQueryResponse` objects

```scala
val responseF: Future[SolrQueryResponse] = (connectionActor ? req).mapTo[SolrQueryResponse]
```

Errors can be raised from Spray (which should be `Http.ConnectionException` errors) or from akka-solr (which should be `Solr.AkkaSolrError`s - `ParseError`, `RequestTimedOut`, etc)

akka-solr provides an `ImperativeWrapper` class that can be wrapped around the client `ActorRef` or requested with:

```scala
val connF: Future[ImperativeWrapper] = Solr.Client.imperativeClientTo("http://mysolrserver")
```

The purpose of `ImperativeWrapper` is to provide a vaguely `SolrServer`-ish interface to akka-solr using Akka `ask`s. This can be helpful to transition from SolrJ or other imperative clients. All `ImperativeWrapper` methods return `Future[SolrQueryResponse]`s.

### Streaming Requests:

akka-solr can use Solr's chunking/streaming mechanism to send query results to an actor as they are received and parsed. The behavior is similar to SolrJ's `SolrServer.queryAndStreamResponse`.

```scala
val req = Solr.Select(qs).streaming
connection ! req
// or
Solr.Client.manager ! Solr.Request(solrUrl, req)
def receive = {
    case SolrResultInfo(numFound, start, maxScore) ⇒ // received first
    case doc: AkkaSolrDocument ⇒ // documents are received
    case res: SolrQueryResponse ⇒ // response is sent last and has no documents; (connection ? req) returns Future[SolrQueryResponse]
}

```

Testability
-----------

We have many unit tests which employ Solr's `EmbeddedSolrServer` to fire up temporary Solr instances that are loaded, queried, updated, and destroyed during testing. I'm sure there's better ways to go about that, but in the interest of maintaining our test setup I've made akka-solr customizable with different connection actors at runtime.

To use a different connection actor, extend the `com.codemettle.akkasolr.ext.ConnectionProvider` trait and configure the `akkasolr.connectionProvider` config to point to your implementation. akka-solr provides an `HttpSolrServerConnectionProvider` in the `"tests"` jar as an example, which uses the akka-solr-provided `SolrServerClientConnection` actor to run queries against a SolrJ `SolrServer`. A simple `ConnectionProvider` can be created in your test code which uses the same `SolrServerClientConnection` actor with an `EmbeddedSolrServer` (example uri: `"solr://embedded?options=that&you=need"`).

Plans
-----

* Tests - I'm not a unit-testing expert, any help is appreciated
* Add more Scala-friendly request/response wrappers as requested.
  * Sub-queries? (can provide queries without using builders)
  * document field weights? (can provide queries without using builders)
  * facet dates/ranges, limiting facets (can use original non-Scala-ish response)
  * spellcheck, highlighting, stats, terms, etc (can use original non-Scala-ish response)
* ???


License
-------

[Apache License, 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)


Changelog
---------

* **2.0.0**
  * Drop support for Solr4, move to Solr 5.1
  * Drop support for Scala 2.10
  * Build with Java8
  * **API Change:** change `isAnyOf` / `isNoneOf` to take iterables instead of being varargs methods (to cut down on `WrappedArray` bugs from forgetting `: _*` vararg conversions)
* **1.5.0**
  * Version change due to breaking API
  * Add support for authentication on regular connections, although not supported (yet?) for LoadBalanced/SolrCloud connections
* **1.0.1**
  * `SolrQueryBuilder` now supports facet pivots, stats, and grouping
  * `isAnyOf` now generates more concise queries (`key:(v1 OR v2 OR v3)` vs `(key:v1 OR key:v2 OR key:v3)`)
  * `Solr.(RequestOptions|UpdateOptions|LBConnectionOptions|SolrCloudConnectionOptions)` now all have a `.materialize(implicit ActorRefFactory)` method to create instances from the ether inside of any actor
  * `SolrQueryStringBuilder` now has an implicit conversion from `Option[QueryPart]`s to `QueryPart`s
  * Bug Fix - nested AND/ORs:a query like `AND(defaultField() := "*", OR(Seq.empty[QueryPart]: _*))` would generate `(* AND )`, now correctly generates `*`
* **1.0.0**
  * Update build to build against 2.10.5 and 2.11.6
  * No code changes, but the project has been in production long enough to mark it as 1.0.
* **0.10.2**
  * Support for cursorMark in `SolrQueryBuilder` and nextCursorMark in `SolrQueryResponse`. Cursors require Solr 4.7.1, but akka-solr hard-codes the constants from SolrJ's `CursorMarkParams` to maintain compatibility with SolrJ < 4.7.1
  * `SolrQueryBuilder.withSortIfNewField(SortClause)`
* **0.10.1**
  * Bugfix - asking for a SolrCloud/LoadBalanced connection with different options but same address as existing would return the existing connection instead of creating a new connection with different connection options (especially visible for SolrCloud connections with different defaultCollection settings)
* **0.10.0**
  * `SolrQueryBuilder.query` is no longer a String, it is a `SolrQueryStringBuilder.QueryPart` for easier modification of queries
  * Responses to operations should now come from the Connection actor instead of the transient Request Handler actors
  * Add the `LBClientConnection` class that behaves pretty much exactly the same as SolrJ's `org.apache.solr.client.solrj.impl.LBHttpSolrServer` class
    * `LBHttpSolrServer` attempts to cycle through servers in order (but the order is changed at runtime when failures happen), `LBClientConnection` uses a random order on every request
    * `LBHttpSolrServer.Req` (lets the user specify a list of servers to try per request that don't necessarily have to be servers that the connection was configured to handle) is reproduced by sending `LBClientConnection.ExtendedRequest` messages to the `LBClientConnection` (`LBHttpSolrServer.Rsp` -> `LBClientConnection.ExtendedResponse`)
  * Add the `SolrCloudConnection` class that behaves pretty much exactly the same as SolrJ's `org.apache.solr.client.solrj.impl.CloudSolrServer` class
    * `CloudSolrServer` has a `setDefaultCollection()` method to set default collections for requests, `SolrCloudConnection`s can be created with a default collection by providing a `Solr.SolrCloudConnectionOptions` instance with `defaultCollection` set; no runtime changes are currently supported
    * `CloudSolrServer` looks for a `"collection"` parameter in requests to override the default (or provide this required piece of data if `setDefaultCollection()` hasn't been called); `SolrCloudConnection` accepts `SolrCloudConnection.OperateOnCollection` messages that provide a per-request collection parameter
    * `Solr.Client.solrCloudClientTo` and its brethern (imperative client, client future) take a host string in the form "host:port,host:port" and accept `SolrCloudConnectionOptions` to set the default collection and other configuration
    * `Solr.Client.clientTo` and its brethern accept connection strings in the form "zk://host:port,host:port" and create a `SolrCloudConnection`; using this method requires that every request be sent in a `SolrCloudConnection.OperateOnCollection` message
* **0.9.2**
  * Add support in SolrQueryBuilder for facetLimit, facetMinCount, and facetPrefix
* **0.9.1**
  * Bug fixes for empty NOT and IsAnyOf query builders
  * add isNoneOf query builder method
* **0.9.0**
  * Initial Release
  * Support for major Solr operations in an actor+message passing interface
  * Support for building immutable messages that represent Solr operations in a Scala-ish manner
  * Testability support through runtime-configurable connection providers, with a provided implementation that can use `EmbeddedSolrServer`s
  * Easier access to Solr output objects through Scala wrappers

Credits
-------
* Authored by [@codingismy11to7](https://github.com/codingismy11to7) for [@CodeMettle](https://github.com/CodeMettle)
  * We've used [@takezoe](https://github.com/takezoe)'s [solr-scala-client](https://github.com/takezoe/solr-scala-client) library extensively in production, and submitted features. akka-solr has no code from solr-scala-client, but there are some superficial similarities. It's a fine library if you need an asynchronous Solr client but don't use Akka.
* Facet pivoting, stats, and grouping `SolrQueryBuilder` support by [@compfix](https://github.com/compfix)
