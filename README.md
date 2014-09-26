akka-solr
=========

A Solr4 client built on [Akka](http://akka.io) and [Spray](http://spray.io)
[![Build Status](https://travis-ci.org/CodeMettle/akka-solr.svg?branch=master)](https://travis-ci.org/CodeMettle/akka-solr)

The goal of akka-solr is to provide a high-performance, non-blocking, Akka-and-Scala-native interface to [Apache Solr4](http://lucene.apache.org/solr/). The initial implementation provides an interface similar to `spray-client`'s, with an Akka extension that allows requests to be sent to an actor, or an interface to request a connection actor and send requests to it. Optional builders for for requests are provided, but are not required; results from Solr are returned as wrapper objects that provide easier access from Scala to SolrJ objects. Some SolrJ objects are used in the interest of maintainability.

#### Note about blocking:
In order to keep from reinventing the wheel and then maintaining said wheel, the SolrJ library is used for generating update (add/delete) requests (which could easily be replaced, and actually is buggy) and for parsing results (`XMLResponseParser`, `BinaryResponseParser`, `StreamingBinaryResponseParser`). Since the SolrJ `ResponseParser`s work from `java.io.InputSource`s, and akka-solr uses reactive/non-blocking response chunking, blocking calls were added to bridge the `InputSource` requests into Akka messages. A dedicated, runtime-configurable executor is used for all SolrJ response parsing with the `ActorInputStream` class and the `akkasolr.response-parser-dispatcher` config. Any improvements / alternate implementations are welcome.


Import
------

akka-solr depends on SolrJ for request generation and response parsing, but the dependency is marked as `"provided"` so the end user is required to pull the dependency in. After cursory inspections, akka-solr is expected to work with SolrJ versions 4.5 through 4.10. Akka and spray-can are not pulled in as `"provided"`, this can be changed if feedback demands.

#### Add Dependencies:

sbt:

```scala
libraryDependencies ++= Seq(
    "com.codemettle.akka-solr" %% "akka-solr" % "0.9.0",
    "org.apache.solr" % "solr-solrj" % "4.5.1" // later versions should work but are untested
)
```

Maven:

```xml
<dependency>
    <groupId>com.codemettle.akka-solr</groupId>
    <artifactId>akka-solr</artifactId>
    <version>0.9.0</version>
</dependency>
<dependency>
    <groupId>org.apache.solr</groupId>
    <artifactId>solr-solrj</artifactId>
    <version>4.5.1</version>
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

```scala
val qs = Solr.queryStringBuilder rawQuery "my custom query"
val qs = Solr.queryStringBuilder defaultField() := "wantthis"
val qs = Solr.queryStringBuilder defaultField() :!= "dontwantthis"
val qs = Solr.queryStringBuilder field "myfield" := "requiredvalue"
val qs = Solr.queryStringBuilder field "myfield" isAnyOf ("1", "2")
val qs = Solr.queryStringBuilder field "mylong" isInRange (12345, 98765)
val qs = Solr.queryStringBuilder field "requiredField" exists()
val qs = Solr.queryStringBuilder field "illegalField" doesNotExist()
```
```scala
import com.codemettle.akkasolr.querybuilder.SolrQueryStringBuilder.Methods._

val qs = Solr.queryStringBuilder AND (
    field ("x") := "y",
    defaultField() :!= "3",
    NOT(field ("z") := 2),
    OR (
        rawQuery ("<my custom query>"),
        field("aa") := "2",
        field("bb") isAnyOf ("1", "2")
    )
)
```

#### Building queries with options

```scala
import com.codemettle.akkasolr.querybuilder.SolrQueryBuilder.FieldStrToSort

val query = qs queryOptions() start 50 rows 25 facets ("a", "b") fields ("f1", "f2") sortBy "f1".desc

```

### Building Requests:

```scala
val req = Solr.Ping(options = Solr.RequestOptions(method = RequestMethods.GET, responseType = SolrResponseTypes.XML, requestTimeout = 5.seconds))

val req = Solr.Ping()
val req = Solr.Ping(Solr.Ping.Enable)

val req = Solr.Commit(waitForSearcher = false, softCommit = true)
val req = Solr.Optimize(waitForSearcher = false, maxSegments = 2)
val req = Solr.Rollback(options = Solr.RequestOptions(method = RequestMethods.POST, responseType = SolrResponseTypes.Binary))

val req = Solr.Select(qs)
val req = Solr.Select.Streaming(qs)
val req = Solr.Select(qs).streaming
val req = Solr.Select(qs).streaming withOptions Solr.RequestOptions(requestTimeout = 15.seconds)

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

Errors can be raised from Spray (which should be `Http.ConnectionException` errors) or from akka-solr (which should be `Solr.AkkaSolrError`s - `InvalidUrl`, `RequestTimedOut`, etc)

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

To use a different connection actor, extend the `com.codemettle.akkasolr.ext.ConnectionProvider` trait and configure the `akkasolr.connectionProvider` config to point to your implementation. akka-solr provides an `HttpSolrServerConnectionProvider` in the `"tests"` jar as an example, which uses the akka-solr-provided `SolrServerClientConnection` actor to run queries against a SolrJ `SolrServer`. A simple `ConnectionProvider` can be created in your test code which uses the same actor with an `EmbeddedSolrServer` (example uri: `"solr://embedded?options=that&you=need"`).

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

* **0.9.0**
  * Initial Release
  * Support for major Solr operations in an actor+message passing interface
  * Support for building immutable messages that represent Solr operations in a Scala-ish manner
  * Testability support through runtime-configurable connection providers, with a provided implementation that can use `EmbeddedSolrServer`s
  * Easier access to Solr output objects through Scala wrappers

Credits
-------
* Authored by [@codingismy11to7](https://github.com/codingismy11to7) for [@CodeMettle](https://github.com/CodeMettle)
  * I have used (and contributed to) [@takezoe](https://github.com/takezoe)'s [solr-scala-client](https://github.com/takezoe/solr-scala-client) library extensively. I submitted a patch to make its query builders immutable-ish; akka-solr's query builders are completely immutable but have some similarities (ie `<query> rows 100 start 0` syntax). I also submitted the capability to use an `EmbeddedSolrServer` for unit testing. A fine library that we have used in production; its only drawback for us was lack of an Akka-native interface and dependency on Netty for async HTTP.
