package com.github.akkasolr

import com.github.akkasolr.ext.SolrExtImpl
import org.apache.solr.common.params.SolrParams

import akka.actor._
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

/**
 * @author steven
 *
 */
object Solr extends ExtensionId[SolrExtImpl] with ExtensionIdProvider {
    def Client(implicit arf: ActorRefFactory) = actorSystem registerExtension this

    override def createExtension(system: ExtendedActorSystem) = new SolrExtImpl(system)

    override def lookup() = Solr

    /***** messages *****/
    case class SolrConnection(forAddress: String, connection: ActorRef)

    sealed trait SolrOperation {
        def timeout: FiniteDuration
    }

    case class Select(query: SolrParams, timeout: FiniteDuration = 1.minute) extends SolrOperation

    case class Ping(action: Option[Ping.Action] = None, timeout: FiniteDuration = 5.seconds) extends SolrOperation

    object Ping {
        sealed trait Action
        case object Enable extends Action
        case object Disable extends Action
    }

    case class RequestTimedOut(after: FiniteDuration)
        extends Exception(s"Request timed out after $after") with NoStackTrace

    case class InvalidResponse(msg: String) extends Exception(s"Couldn't handle response: $msg") with NoStackTrace

    case class ParseError(t: Throwable) extends Exception("Error parsing response", t) with NoStackTrace
}
