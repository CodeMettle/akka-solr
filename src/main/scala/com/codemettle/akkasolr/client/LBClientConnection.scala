/*
 * LBClientConnection.scala
 *
 * Updated: Oct 16, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
package com.codemettle.akkasolr
package client

import org.apache.solr.client.solrj.SolrQuery

import com.codemettle.akkasolr.Solr.{LBConnectionOptions, SolrOperation}
import com.codemettle.akkasolr.client.LBClientConnection._
import com.codemettle.akkasolr.querybuilder.SolrQueryBuilder.ImmutableSolrParams
import com.codemettle.akkasolr.solrtypes.{AkkaSolrDocument, SolrQueryResponse, SolrResultInfo}
import com.codemettle.akkasolr.util.Util

import akka.actor._
import akka.http.scaladsl.model.{StatusCode, StatusCodes, Uri}
import akka.stream.StreamTcpException
import scala.concurrent.duration._
import scala.util.Random

/**
 * An actor for load-balancing requests between Solr servers. Based directly on [[org.apache.solr.client.solrj.impl.LBHttpSolrClient]]
 * so its notes on indexing hold for this implementation. Isn't strictly round robin (and neither is the original, due
 * to error handling and retries), but tries servers in random order for every request (so roughly equal distribution
 * over time). The random query sequences always try "zombie" servers after all "live" servers have been tried. Doesn't
 * provide ability to add/remove servers after creation, but could easily be added. The capability to query all servers
 * in parallel and return the first successful response isn't currently implemented, but could easily be added.
 *
 * @see [[org.apache.solr.client.solrj.impl.LBHttpSolrClient]]
 * @author steven
 *
 */
object LBClientConnection {
    def props(servers: Iterable[Solr.SolrConnection], options: LBConnectionOptions) =
        Props(new LBClientConnection(servers, options))

    /**
     * Message that runs an operation analogously to [[org.apache.solr.client.solrj.impl.LBHttpSolrClient#request(org.apache.solr.client.solrj.impl.LBHttpSolrClient.Req)]]
     *
     * @param op operation to run
     * @param servers a list of servers to try that don't necessarily have to be the same servers that the
     *                [[LBClientConnection]] was created to handle
     * @param numDeadServersToTry The number of dead servers to try if there are no live servers left. Defaults to
     *                            the number of servers in this request if the number is less than 0.
     */
    @SerialVersionUID(1L)
    case class ExtendedRequest(op: SolrOperation, servers: List[String], numDeadServersToTry: Int = -1)

    @SerialVersionUID(1L)
    case class ExtendedResponse(response: SolrQueryResponse, server: String)

    private val emptyQuery = {
        val q = new SolrQuery("*:*")
        q setRows 0
        ImmutableSolrParams(q)
    }

    private val deadServerErrors: Set[StatusCode] = Set(StatusCodes.NotFound, StatusCodes.Forbidden,
        StatusCodes.ServiceUnavailable, StatusCodes.InternalServerError)

    private case object CheckAlive

    private case class ConnectionWrapper(originalUrl: String, uri: Uri, connection: ActorRef,
                                         standard: Boolean = true, failedPings: Int = 0)

    private case class ZombieCheckFinished(sucessfullyConnected: Boolean, server: ConnectionWrapper)

    private case class ServerSuccessfullyQueried(serverUri: Uri)

    private case class DeadServerDetected(serverUri: Uri, origUrl: String, connection: ActorRef)

    private class ZombieChecker(server: ConnectionWrapper) extends Actor {
        override def preStart(): Unit = {
            super.preStart()

            server.connection ! (Solr.Select(emptyQuery) withTimeout 10.seconds)
        }

        private def sendStatus(successful: Boolean) = {
            context.parent ! ZombieCheckFinished(successful, server)
            context stop self
        }

        override def receive: Receive = {
            case resp: SolrQueryResponse if resp.status == 0 => sendStatus(successful = true)
            case _ => sendStatus(successful = false) // resp.status could be non-zero or it raised an exception
        }
    }

    private object ZombieChecker {
        def props(server: ConnectionWrapper) = Props(new ZombieChecker(server))
    }

    private case class ExtServerInfo(uri: Uri, orig: String)
    private type ExtServer = Either[ExtServerInfo, ConnectionWrapper]

    private case object ReqTimeout

    private type Servers = List[ConnectionWrapper]
    private type ExtServers = List[ExtServer]

    private abstract class RequestRunner[Server, RespType](op: Solr.SolrOperation, replyTo: ActorRef,
                                                           initAlive: List[Server], initDead: Servers) extends Actor {
        import context.dispatcher

        private val startTime = System.nanoTime()
        private val hardTimeout = actorSystem.scheduler.scheduleOnce(op.requestTimeout + 1.second, self, ReqTimeout)
        private var streaming = false

        protected def createResponse(from: SolrQueryResponse, current: Server): RespType
        protected def sendOperation(curr: Server, op: Solr.SolrOperation): Unit
        protected def deadServer(s: ConnectionWrapper): Server
        protected def serverInfo(s: Server, msgSender: ActorRef): (Uri, String, ActorRef)

        override def preStart(): Unit = {
            super.preStart()

            tryNext(startTime, initAlive, initDead)
        }

        override def postStop(): Unit = {
            super.postStop()

            hardTimeout.cancel()
        }

        private def sendStreamingMessage(m: Any) = {
            // if the user is making a streaming query, then as soon as any streaming message comes in mark this as a
            //   streaming request, and return the result whether it's an error or not
            replyTo ! m
            streaming = true
        }

        private def tryNext(currNanoTime: Long, alive: List[Server], dead: Servers) = {
            val elapsedSinceStart = (currNanoTime - startTime).nanos
            val newTimeout = op.requestTimeout - elapsedSinceStart
            def requestWithTimeout = op withTimeout newTimeout

            if (newTimeout > Duration.Zero) {
                // will almost always be the case, but if not then a hard timeout msg will be coming in
                if (alive.isEmpty && dead.isEmpty) {
                    replyTo ! Status.Failure(Solr.AllServersDead())
                    context stop self
                } else if (alive.nonEmpty) {
                    val curr = alive.head
                    sendOperation(curr, requestWithTimeout)
                    context become sendingBehavior(curr, alive.tail, dead)
                } else {
                    val curr = dead.head
                    curr.connection ! requestWithTimeout
                    context become sendingBehavior(deadServer(curr), alive, dead.tail)
                }
            }
        }

        private def replyWithResult(result: Any, curr: Server, msgSender: ActorRef) = {
            replyTo ! result
            val (uri, _, _) = serverInfo(curr, msgSender)
            context.parent ! ServerSuccessfullyQueried(uri)
            context stop self
        }

        private def errorMeansServerIsDead(t: Throwable): Boolean = t match {
            case _: Solr.ConnectionException => true
            case _: StreamTcpException => true
            case Solr.ServerError(status, _) if deadServerErrors(status) => true
            case _ => false
        }

        private def handleError(t: Throwable, errorFrom: ActorRef, curr: Server, alive: List[Server], dead: Servers) = {
            if (streaming) {
                replyTo ! Status.Failure(t)
                context stop self
            } else {
                if (errorMeansServerIsDead(t)) {
                    val (uri, origUrl, conn) = serverInfo(curr, errorFrom)
                    context.parent ! DeadServerDetected(uri, origUrl, conn)
                    tryNext(System.nanoTime(), alive, dead)
                } else {
                    replyWithResult(Status.Failure(t), curr, errorFrom)
                }
            }
        }

        private def handleTimeout: Receive = {
            case ReqTimeout =>
                // this is only a fail-safe, the underlying connection should time out the request if the limit is hit
                replyTo ! Status.Failure(Solr.RequestTimedOut(op.requestTimeout))
                context stop self
        }

        override def receive: Receive = handleTimeout

        def sendingBehavior(current: Server, alive: List[Server], dead: Servers): Receive = handleTimeout orElse {
            case d: AkkaSolrDocument => sendStreamingMessage(d)
            case ri: SolrResultInfo => sendStreamingMessage(ri)
            case sqr: SolrQueryResponse => replyWithResult(createResponse(sqr, current), current, sender())
            case Status.Failure(t) => handleError(t, sender(), current, alive, dead)
        }
    }

    private class StandardRequestRunner(op: Solr.SolrOperation, replyTo: ActorRef, initAlive: Servers,
                                        initDead: Servers)
        extends RequestRunner[ConnectionWrapper, SolrQueryResponse](op, replyTo, initAlive, initDead) {

        protected def createResponse(from: SolrQueryResponse, current: ConnectionWrapper): SolrQueryResponse = from
        protected def deadServer(s: ConnectionWrapper): ConnectionWrapper = s
        protected def serverInfo(s: ConnectionWrapper, msgSender: ActorRef): (Uri, String, ActorRef) = (s.uri, s.originalUrl, s.connection)
        protected def sendOperation(curr: ConnectionWrapper, op: SolrOperation): Unit = curr.connection ! op
    }

    private object StandardRequestRunner {
        def props(op: Solr.SolrOperation, replyTo: ActorRef, alive: Vector[ConnectionWrapper],
                  standardDead: Vector[ConnectionWrapper]) =
            Props(new StandardRequestRunner(op, replyTo, Random.shuffle(alive).toList,
                Random.shuffle(standardDead).toList))
    }

    private class ExtendedRequestRunner(op: Solr.SolrOperation, replyTo: ActorRef, initAlive: ExtServers,
                                        initDead: Servers, solrManager: ActorRef)
        extends RequestRunner[ExtServer, ExtendedResponse](op, replyTo, initAlive, initDead) {
        protected def createResponse(from: SolrQueryResponse, current: ExtServer): ExtendedResponse = {
            current match {
                case Left(info) => ExtendedResponse(from, info.orig)
                case Right(cw) => ExtendedResponse(from, cw.originalUrl)
            }
        }
        protected def deadServer(s: ConnectionWrapper) = Right(s)
        protected def serverInfo(s: ExtServer, msgSender: ActorRef): (Uri, String, ActorRef) = s match {
            case Left(info) => (info.uri, info.orig, msgSender)
            case Right(cw) => (cw.uri, cw.originalUrl, cw.connection)
        }
        protected def sendOperation(curr: ExtServer, op: SolrOperation): Unit = curr match {
            case Left(info) => solrManager ! Solr.Request(info.orig, op)
            case Right(cw) => cw.connection ! op
        }
    }

    private object ExtendedRequestRunner {
        def props(op: Solr.SolrOperation, replyTo: ActorRef, initAlive: Vector[ExtServer],
                  initDead: Vector[ConnectionWrapper], solrManager: ActorRef) =
            Props(new ExtendedRequestRunner(op, replyTo, Random.shuffle(initAlive).toList,
                Random.shuffle(initDead).toList, solrManager))
    }
}

class LBClientConnection(servers: Iterable[Solr.SolrConnection], options: LBConnectionOptions)
    extends Actor with ActorLogging {
    import context.dispatcher

    val aliveCheckInterval = options.aliveCheckInterval
    val nonStandardPingLimit = options.nonStandardPingLimit

    val aliveCheckTimer = actorSystem.scheduler.schedule(aliveCheckInterval, aliveCheckInterval, self, CheckAlive)

    private var aliveServers = (servers map connectionToWrapper).toVector
    private var zombieServers = Vector.empty[ConnectionWrapper]

    override def postStop(): Unit = {
        super.postStop()

        aliveCheckTimer.cancel()
    }

    private def connectionToWrapper(conn: Solr.SolrConnection) = {
        val addr = conn.forAddress
        ConnectionWrapper(addr, Util normalize addr, conn.connection)
    }

    private def updateZombie(z: ConnectionWrapper, f: (ConnectionWrapper) => ConnectionWrapper) = {
        val idx = zombieServers indexWhere (_.uri == z.uri)
        if (idx >= 0)
            zombieServers = zombieServers.updated(idx, f(zombieServers(idx)))
    }

    private def zombieCheckFinished(successful: Boolean, server: ConnectionWrapper) = {
        // "standard" servers are ones that this LB connection was configured to connect to at creation time
        // zombie checking is done on both standard servers and ones provided by extended requests
        // zombies that can be (re)connected to are added to our aliveServers only if they're "standard",
        //   otherwise they're just removed from the zombieServers fail-fast collection
        // after a non-standard zombie is pinged (and fails) `nonStandardPingLimit` times, it is removed from the
        //   fail-fast collection (and so they stop being checked)

        // this only runs if the given server is, in fact, a zombie
        zombieServers find (_.uri == server.uri) foreach (z => {
            if (successful) {
                removeFromDead(z.uri)
                if (z.standard)
                    addToAlive(z)
            } else {
                updateZombie(z, _.copy(failedPings = 1 + z.failedPings))
                if (!z.standard && ((1 + z.failedPings) >= nonStandardPingLimit))
                    removeFromDead(z.uri)
            }
        })
    }

    private def removeFromDead(key: Uri): Unit = {
        zombieServers = zombieServers filterNot (_.uri == key)
    }

    private def handleServerIsAlive(serverUri: Uri): Unit = {
        // the only case we care about is if this server is in the zombies list
        zombieServers find (_.uri == serverUri) foreach (z => {
            // always remove from dead
            zombieServers = zombieServers filterNot (_ == z)
            // but only add it to alive if it's standard
            if (z.standard)
                addToAlive(z)
        })
    }

    private def handleServerIsDead(serverUri: Uri, origUrl: String, connection: ActorRef): Unit = {
        // the class currently doesn't handle adding/removing "standard" servers, but write this method as if it does

        def addToDeadIfNeeded(conn: => ConnectionWrapper) = {
            zombieServers find (_.uri == serverUri) match {
                case None =>
                    log debug ("burying {}", conn.uri)
                    zombieServers :+= conn

                case Some(s) =>
                    val newDead = conn
                    // this won't happen unless we enable adding/removing servers and we get a change while a
                    // request is in-flight and the request errors out and the request was to a newly-added server
                    if (!s.standard && newDead.standard) {
                        removeFromDead(s.uri)
                        zombieServers :+= newDead
                    }
            }
        }

        aliveServers find (_.uri == serverUri) match {
            case Some(cw) =>
                aliveServers = aliveServers filterNot (_ == cw)
                addToDeadIfNeeded(cw)

            case None => addToDeadIfNeeded(ConnectionWrapper(origUrl, serverUri, connection, standard = false))
        }
    }

    private def addToAlive(wrapper: ConnectionWrapper) = {
        if (!aliveServers.exists(_.uri == wrapper.uri)) {
            log debug ("{} rose from the dead", wrapper.uri)

            aliveServers :+= wrapper.copy(failedPings = 0)
        }
    }

    private def runExtendedRequest(op: Solr.SolrOperation, serverStrs: List[String], numDeadToTry: Int) = {
        // collapse all server urls into unique URIs and (one of) their corresponding URL
        val urisAndOrigStrs = serverStrs.foldLeft(Map.empty[Uri, String]) {
            case (acc, str) => acc + (Util.normalize(str) -> str)
        }

        // separate out specified servers that we know are zombies from ones that we don't know about or are known good
        val (zombies, rest) = urisAndOrigStrs.foldLeft(Vector.empty[ConnectionWrapper] -> Vector.empty[ExtServer]) {
            case ((accZombies, accToTry), (serverUri, serverStr)) =>
                def liveOrExtendedServer: ExtServer = {
                    val existing = aliveServers find (_.uri == serverUri)
                    existing.fold[ExtServer](Left(ExtServerInfo(serverUri, serverStr)))(Right(_))
                }

                zombieServers find (_.uri == serverUri) match {
                    case None => accZombies -> (accToTry :+ liveOrExtendedServer)
                    case Some(z) => (accZombies :+ z) -> accToTry
                }
        }

        context.actorOf(ExtendedRequestRunner.props(op, sender(), rest, zombies, context.parent))
    }

    override def receive: Receive = {
        case CheckAlive => zombieServers foreach (z => context.actorOf(ZombieChecker props z))

        case ZombieCheckFinished(s, srv) => zombieCheckFinished(s, srv)

        case op: Solr.SolrOperation =>
            // the request runner will reply to the sender and also send us any dead/alive messages
            context.actorOf(StandardRequestRunner.props(op, sender(), aliveServers, zombieServers filter (_.standard)))

        case ExtendedRequest(op, serverStrs, numDeadToTry) => runExtendedRequest(op, serverStrs, numDeadToTry)

        case ServerSuccessfullyQueried(serverUri) => handleServerIsAlive(serverUri)

        case DeadServerDetected(uri, url, conn) => handleServerIsDead(uri, url, conn)
    }
}
