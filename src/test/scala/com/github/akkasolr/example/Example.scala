package com.github.akkasolr.example

import com.github.akkasolr.Solr
import com.github.akkasolr.client.ClientConnection
import net.ceedubs.ficus.Ficus._
import org.apache.solr.client.solrj.SolrQuery

import akka.actor._
import scala.concurrent.duration._

/**
 * @author steven
 *
 */
object Example extends App {
    val system = ActorSystem("Example")

    val main = system.actorOf(Props[MyAct])
    system.actorOf(Props[WD])

    private class MyAct extends Actor with ActorLogging {
        private var conn: ActorRef = _

        private val config = context.system.settings.config.getConfig("example")

        override def preStart() = {
            super.preStart()

            import context.dispatcher

            context.system.scheduler.scheduleOnce(35.seconds, self, 'q)
            context.system.scheduler.scheduleOnce(2.minutes, self, PoisonPill)

            Solr.Client.clientTo(config.as[String]("solrAddr"))
        }

        private def sendQuery() = {
            conn ! ClientConnection.Messages.Select(new SolrQuery(config.as[String]("testQuery")))
        }

        def receive = {
            case Solr.SolrConnection(a, c) ⇒
                log.info("Got {} for {} from {}", c, a, sender())
                conn = sender()
                sendQuery()

            case 'q ⇒ sendQuery()

            case m ⇒
                log.info("got {}", m)
        }
    }

    private class WD extends Actor {
        context watch main
        def receive = {
            case Terminated(`main`) ⇒ system.shutdown()
        }
    }
}
