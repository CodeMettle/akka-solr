package com.github.akkasolr.example

import akka.actor._
import scala.concurrent.duration._

/**
 * @author steven
 *
 */
object Example extends App {
    val system = ActorSystem("Example")
    import com.github.akkasolr.example.Example.system.dispatcher

    val act = system.actorOf(Props[Actor](new Actor with ActorLogging {

        override def postStop() = {
            super.postStop()

            context.system.shutdown()
        }

        private def doit() = {
            log debug "starting child"

            val c = context.actorOf(Props[Actor](new Actor with ActorLogging {
                override def preStart() = {
                    super.preStart()

                    context.system.scheduler.scheduleOnce(500.millis, self, PoisonPill)

                    log info "child started"
                }

                def receive = Actor.emptyBehavior
            }), "child")

            context watch c
        }

        def receive = {
            case 'doit ⇒
                log info "This is something happening"
                doit()

            case Terminated(_) ⇒
                log debug "child died, shutting down"
                context.system.scheduler.scheduleOnce(500.millis, self, PoisonPill)
        }
    }), "testActor")

    system.scheduler.scheduleOnce(500.millis, act, 'doit)
}
