package akka.http

import com.typesafe.config.Config

import com.codemettle.akkasolr.Solr._

import akka.http.impl.util.SettingsCompanion
import scala.concurrent.duration._

/**
  * Created by steven on 6/7/2017.
  */
object SettingsHack {
  object RequestOptionsHack extends SettingsCompanion[RequestOptions]("akkasolr.request-defaults") {
    override def fromSubConfig(root: Config, c: Config): RequestOptions =
      RequestOptions(
        c getString "method" match {
          case "GET" ⇒ RequestMethods.GET
          case "POST" ⇒ RequestMethods.POST
          case m ⇒ throw new IllegalArgumentException(s"Invalid akkasolr.request-defaults.method: $m")
        },
        c getString "writer-type" match {
          case "XML" ⇒ SolrResponseTypes.XML
          case "Binary" ⇒ SolrResponseTypes.Binary
          case "Streaming" ⇒ SolrResponseTypes.Streaming
          case m ⇒ throw new IllegalArgumentException(s"Invalid akkasolr.request-defaults.writer-type: $m")
        },
        c.getDuration("request-timeout", MILLISECONDS).millis.toCoarsest match {
          case fd: FiniteDuration ⇒ fd
          case o ⇒
            throw new IllegalArgumentException(s"Invalid akkasolr.request-defaults.request-timeout: $o")
        }
      )
  }

  object UpdateOptionsHack extends SettingsCompanion[UpdateOptions]("akkasolr.update-defaults") {
    override def fromSubConfig(root: Config, c: Config): UpdateOptions =
      UpdateOptions(
        c getBoolean "commit",
        if (!c.hasPath("commit-within") || c.getString("commit-within") == "infinite") None
        else Some(c.getDuration("commit-within").toNanos.nanos),
        c getBoolean "overwrite"
      )
  }

  object LBConnectionOptionsHack extends SettingsCompanion[LBConnectionOptions]("akkasolr.load-balanced-connection-defaults") {
    override def fromSubConfig(root: Config, c: Config): LBConnectionOptions =
      LBConnectionOptions(
        c.getDuration("alive-check-interval").toNanos.nanos,
        c getInt "non-standard-ping-limit"
      )
  }

  object SolrCloudConnectionOptionsHack extends SettingsCompanion[SolrCloudConnectionOptions]("akkasolr.solrcloud-connection-defaults") {
    override def fromSubConfig(root: Config, c: Config): SolrCloudConnectionOptions =
      SolrCloudConnectionOptions(
        c.getDuration("zookeeper-connect-timeout").toNanos.nanos,
        c.getDuration("zookeeper-client-timeout").toNanos.nanos,
        c getBoolean "connect-at-start",
        Option(c getString "default-collection") flatMap (s ⇒ if (s.trim.isEmpty) None else Some(s)),
        c getBoolean "parallel-updates",
        c getString "id-field"
      )
  }

}
