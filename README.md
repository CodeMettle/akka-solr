akka-solr
=========

A Solr4 client built on [Akka](http://akka.io) and [Spray](http://spray.io)
[![Build Status](https://travis-ci.org/CodeMettle/akka-solr.svg?branch=master)](https://travis-ci.org/CodeMettle/akka-solr)

The goal of akka-solr is to provide a high-performance, non-blocking, Akka-and-Scala-native interface to [Apache Solr4](http://lucene.apache.org/solr/). The initial implementation provides an interface similar to `spray-client`'s, with an Akka extension that allows requests to be sent to an actor, or an interface to request a connection actor and send requests to it. Optional builders for for requests are provided, but are not required; results from Solr are returned as wrapper objects that provide easier access from Scala to SolrJ objects. Some SolrJ objects are used in the interest of maintainability.

#### Note about blocking:
In order to keep from reinventing the wheel and then maintaining said wheel, the SolrJ library is used for generating update (add/delete) requests (which could easily be replaced, and actually is buggy) and for parsing results (`XMLResponseParser`, `BinaryResponseParser`, `StreamingBinaryResponseParser`). Since the SolrJ `ResponseParser`s work from `java.io.InputSource`s, and akka-solr uses reactive/non-blocking response chunking, blocking calls were added to bridge the `InputSource` requests into Akka messages. A dedicated, runtime-configurable executor is used for all SolrJ response parsing with the `ActorInputStream` class and the `akkasolr.response-parser-dispatcher` config. Any improvements / alternate implementations are welcome.

--------------

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

-------------

Usage
-----

If you've used the `spray-can` HTTP client library, you're already familiar with akka-solr's philosophy.

Some scaladocs are provided, open a ticket for anything unclear (or submit a pull request!).

In lieu of detailed documentation, here are a list of examples (using `ask`/`?` syntax for clarity and brevity, even though the library is meant to be used from Actors with message passing).

-------------

Plans
-----

* Add more Scala-friendly request/response wrappers as requested.
* ???

-------------

License
-------

[Apache License, 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

-------------

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