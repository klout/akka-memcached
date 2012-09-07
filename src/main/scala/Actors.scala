package com.klout.akkamemcache

import ActorTypes._

import akka.actor._
import akka.dispatch.Future
import akka.event.Logging
import akka.routing._
import akka.util.ByteString
import com.google.common.hash.Hashing._
import com.klout.akkamemcache.Protocol._
import com.klout.akkamemcache.Protocol._
import java.io._
import java.net.InetSocketAddress
import java.net.URLEncoder._
import scala.collection.JavaConversions._
import scala.collection.mutable.{ HashMap, LinkedHashSet }
import scala.util.Random

/**
 * These types are used to make the code more understandable.
 */
object ActorTypes {
    type RequestingActorRef = ActorRef
    type IoActorRouterRef = ActorRef
    type PoolActorRef = ActorRef
}

/**
 * This actor instantiates the pool of MemcachedIOActors and routes requests
 * from the MemcachedClient to the IOActors.
 */
class PoolActor(hosts: List[(String, Int)], connectionsPerServer: Int) extends Actor {

    val hashFunction = goodFastHash(32)

    /**
     * Maps memcached servers to a pool of IOActors, one for each connection.
     */
    var ioActorMap: Map[String, IoActorRouterRef] = _

    /**
     * RequestMap maps the requesting actor to the results that will be recieved from memcached.
     */
    val requestMap: HashMap[RequestingActorRef, HashMap[String, Option[GetResult]]] = new HashMap()

    /**
     * Updates the requestMap to add the result from Memcached to any actor
     * that requested it.
     */
    def updateRequestMap(result: GetResult) = {
        requestMap ++= requestMap map {
            case (actor, resultMap) => {
                val newResultMap = resultMap map {
                    case (key, resultOption) if key == result.key => (key, Some(result))
                    case other                                    => other
                }
                (actor, newResultMap)
            }
        }
    }

    /**
     * If the all of the results from Memcached have been returned for a given actor, this
     * function will send the results to the actor and remove the actor from the requestMap
     */
    def sendResponses() {
        val responsesToSend = requestMap flatMap {
            case (actor, resultMap) if (!resultMap.values.toList.contains(None)) => Some(actor, resultMap)
            case other => None
        }
        responsesToSend foreach {
            case (actor, responses) =>
                actor ! responses.values.flatten
                requestMap -= actor
        }
    }

    /**
     * Instantiate the actors for the Memcached clusters. Each host is mapped to a set
     * of actors. Each IoActor owns one connection to the server.
     */
    override def preStart {
        ioActorMap =
            hosts.map {
                case (host, port) =>
                    val ioActors = (1 to connectionsPerServer).map {
                        num =>
                            context.actorOf(Props(new MemcachedIOActor(host, port, self)),
                                name = encode("Memcached IoActor for " + host + " " + num))
                    }.toList
                    val router = RoundRobinRouter(routees = ioActors)
                    val routingActor = context.actorOf(Props(new MemcachedIOActor(host, port, self)) withRouter router,
                        name = encode("Memcached IoActor Router for " + host))
                    host -> routingActor
            } toMap
    }

    /**
     * Splits the given command into subcommands that are sent to the
     * appropriate IoActors.
     */
    def forwardCommand(command: Command) = {
        val hostCommandMap = command match {
            case SetCommand(keyValueMap, ttl) =>
                val splitKeyValues = keyValueMap groupBy {
                    case (key, value) =>
                        val hashCode = hashFunction hashString key
                        hosts(consistentHash(key.hashCode, hosts.size))
                }
                splitKeyValues map {
                    case (host, keyValueMap) => (host, SetCommand(keyValueMap, ttl))
                }

            case GetCommand(keys) =>
                val splitKeys = keys groupBy (key => hosts(consistentHash(key.hashCode, hosts.size)))
                splitKeys map {
                    case (host, keys) => (host, GetCommand(keys))
                }

            case command: DeleteCommand =>
                val splitKeys = command.keys groupBy (key => hosts(consistentHash(key.hashCode, hosts.size)))
                splitKeys map {
                    case (host, keys) => (host, DeleteCommand(keys: _*))
                }
        }

        hostCommandMap foreach {
            case ((host, port), command) => ioActorMap(host) ! command
        }
    }

    def receive = {
        /**
         * For GetCommands, this will save the requester to the requestMap so the
         * result can be returned to the requester.
         */
        case command @ GetCommand(keys) =>
            val keyResultMap = keys.map {
                key => key -> None
            }.toList
            requestMap += ((sender, HashMap(keyResultMap: _*)))
            forwardCommand(command)

        /* Route a SetCommand or DeleteCommand to the correct IoActor */
        case command: Command => forwardCommand(command)

        /**
         * Update the requestMap for any actors that were requesting this result, and
         * send responses to the actors if their request has been fulfilled.
         */
        case result: GetResult =>
            updateRequestMap(result)
            sendResponses()

        case GetResults(results) =>
            results foreach updateRequestMap
            sendResponses()

    }
}

/**
 * This actor is responsible for all communication to and from a single memcached server
 * using a single connection.
 */
class MemcachedIOActor(host: String, port: Int, poolActor: PoolActorRef) extends Actor {

    val log = Logging(context.system, this)
    var connection: IO.SocketHandle = _

    /**
     * The maximum number of keys that can be queried in a single multiget
     */
    val maxKeyLimit = 1000

    /**
     * Contains the pending results for a Memcache multiget that is currently
     * in progress
     */
    val currentSet: LinkedHashSet[String] = new LinkedHashSet()

    /**
     * Contains the pending results for the next Memcached multiget
     */
    val nextSet: LinkedHashSet[String] = new LinkedHashSet()

    /**
     * Opens a single connection to the Memcached server
     */
    override def preStart {
        connection = IOManager(context.system) connect new InetSocketAddress(host, port)
        log.debug("IoActor starting on " + host + ":" + port)
    }

    /**
     * Adds this get request to the IOActor's internal state. If there is a get currently
     * in progress, the request is placed in a queued map, and will be executed after the
     * current request is completed
     */
    def enqueueCommand(keys: Set[String]) {
        /* Remove duplicate keys */
        val newKeys = keys diff (nextSet ++ currentSet)
        val set = if (awaitingResponseFromMemcached) nextSet else currentSet

        set ++= newKeys

    }

    private def empty[T](set: LinkedHashSet[T]) {
        set --= set
    }

    /**
     * Writes a multiget command that contains all of the keys from currentMap
     * into Memcached
     */
    def writeGetCommandToMemcachedIfPossible() {
        if (!awaitingResponseFromMemcached) {
            if (currentSet.size > 0) {
                connection write GetCommand(currentSet toSet).toByteString
                awaitingResponseFromMemcached = true
            } else {
                awaitingResponseFromMemcached = false
            }
        }
    }

    /**
     * This is triggered when Memcached sends an END. At this point, any keys remaining
     * in currentSet are cache misses.
     */
    def getCommandCompleted() {
        awaitingResponseFromMemcached = false

        /* Send "NotFound" messages for any keys remaining in the current set */
        poolActor ! GetResults(currentSet.map(NotFound).toSet)

        empty(currentSet)

        /* Copy the queued instructions to the current set */
        currentSet ++= (nextSet take maxKeyLimit)

        /* Remove those instructions from the queue */
        nextSet --= (nextSet take maxKeyLimit)

        /* Write the current set of get requests */
        writeGetCommandToMemcachedIfPossible()
    }

    /**
     * This Iteratee processes the responses from Memcached and sends messages back to
     * the IoActor whenever it has parsed a result
     */
    val iteratee = IO.IterateeRef.async(new Iteratees(self).processInput)(context.dispatcher)

    /**
     * If this actor is awaiting a response from Memcached, then it will not
     * make any requests until the response is returned
     */
    var awaitingResponseFromMemcached = false

    def receive = {
        case raw: ByteString => connection write raw

        /**
         * Adds the keys for the getcommand to a queue for writing to Memcached,
         * and issues the command if the actor is not currently waiting for a
         * response from Memcached
         */
        case GetCommand(keys) =>
            enqueueCommand(keys)
            writeGetCommandToMemcachedIfPossible()

        /**
         * Immediately writes a command to Memcached
         */
        case command: Command       => connection write command.toByteString

        /**
         * Reads data from Memcached. The iteratee will send the result
         * of this read to this actor as a Found or Finished message
         */
        case IO.Read(socket, bytes) => iteratee(IO Chunk bytes)

        /**
         * A single key-value pair has been returned from Memcached. Sends
         * the result to the poolActor and removes the key from the set of keys
         * that don't currently have a value
         */
        case found @ Found(key, value) =>
            poolActor ! found
            currentSet -= key

        /**
         * A get command has finished. This will send the appropriate message
         * to the poolActor and make another command if necessary
         */
        case Finished => getCommandCompleted()
    }

}

/**
 * Stores the result of a Memcached Get
 */
sealed trait GetResult {
    def key: String
}

/**
 * Contains a set of GetResults. This case class is necessary to compensate
 * for JVM type erasure
 */
case class GetResults(results: Set[GetResult])

/**
 * Cache Hit
 */
case class Found(key: String, value: Array[Byte]) extends GetResult

/**
 * Cache Miss
 */
case class NotFound(key: String) extends GetResult
