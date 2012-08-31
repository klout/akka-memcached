package com.klout.akkamemcache

import akka.actor._
import akka.util.ByteString
import java.net.InetSocketAddress
import akka.dispatch.Future
import com.klout.akkamemcache.Protocol._
import scala.collection.mutable.{ HashMap, HashSet }
import com.klout.akkamemcache.Protocol._
import scala.collection.JavaConversions._
import scala.util.Random
/**
 * This actor instantiates the pool of MemcachedIOActors and routes requests
 * from the MemcachedClient to the IOActors.
 */
class PoolActor(hosts: List[(String, Int)]) extends Actor {

    val connectionsPerServer = 3

    /**
     * This is a mapping from hosts to actors. Each host has one actor for each connection
     * to it
     */
    var ioActors: HashMap[String, List[ActorRef]] = _

    /**
     * RequestMap maps actors to the results that the actor has recieved from memcached.
     */
    var requestMap: HashMap[ActorRef, HashMap[String, Option[GetResult]]] = new HashMap()

    /**
     * Updates the requestMap to add the result from Memcached to any actor
     * that requested it.
     */
    def updateRequestMap(result: GetResult) = {
        requestMap = requestMap map {
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
        val responsesToSend = requestMap.flatMap{
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
        ioActors = HashMap{
            hosts.map {
                case (host, port) =>
                    (host, (1 to connectionsPerServer).map {
                        num =>
                            context.actorOf(Props(new MemcachedIOActor(host, port, self)),
                                name = "Memcached_IO_Actor_for_" + host + "_" + num)
                    }.toList)
            }: _*
        }
    }

    /**
     * Splits the given command into subcommands that are sent to the
     * appropriate IoActors.
     */
    def forwardCommand(command: Command) = {
        command.consistentSplit(hosts) foreach {
            case ((host, port), command) => {
                // Send the command to a random connection on the appropriate server
                val ioActor = ioActors(host)(Random.nextInt(connectionsPerServer))
                ioActor ! command
            }
        }
    }

    def receive = {
        /**
         * For GetCommands, send a reference to the requesting actor to the
         * IoActor so that the MemcachedIoActor can return the result directly
         * to the requester. Because SetCommands and DeleteCommands are fire-and-forget,
         * the MemcachedIoActor does not need to know the sender.
         */
        case command @ GetCommand(keys) =>
            val keyResultMap = keys.map {
                key => key -> None
            }.toList
            requestMap += ((sender, HashMap(keyResultMap: _*)))
            forwardCommand(command)

        /* Route a SetCommand or GetCommand to the correct IoActor */
        case command: Command => forwardCommand(command)

        /**
         * Update the requestMap for any actors that were requesting this result, and
         * send responses to the actors if any actors request has been fulfilled.
         */
        case result: GetResult => {
            updateRequestMap(result)
            sendResponses()
        }
        case results: Set[GetResult] => {
            results foreach updateRequestMap
            sendResponses()
        }
    }
}

/**
 * This actor is responsible for all communication to and from a single memcached server
 * using a single conneciton.
 */
class MemcachedIOActor(host: String, port: Int, poolActor: ActorRef) extends Actor {
    var connection: IO.SocketHandle = _

    /* Contains the pending results for a Memcache multiget that is currently
     * in progress */
    var currentSet: HashSet[String] = new HashSet()

    /* Contains the pending results for the next Memcached multiget */
    var nextSet: HashSet[String] = new HashSet()

    /**
     * Opens a single connection to the Memcached server
     */
    override def preStart {
        connection = IOManager(context.system) connect new InetSocketAddress(host, port)
    }

    /**
     * Adds this get request to the IOActor's internal state. If there is a get currently
     * in progress, the request is placed in a queued map, and will be executed after the
     * current request is completed
     */
    def enqueueCommand(keys: Set[String]) {
        val set = if (awaitingResponseFromMemcached) nextSet else currentSet
        set ++= keys
    }

    /**
     * Writes a multiget command that contains all of the keys from currentMap
     * into Memcached
     */
    def writeGetCommandToMemcachedIfPossible() {
        if (!awaitingResponseFromMemcached) {
            if (currentSet.size > 0) {
                connection.write(GetCommand(currentSet.toSet).toByteString)
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
        poolActor ! currentSet.map(NotFound).toSet
        currentSet = nextSet
        nextSet = HashSet.empty
        writeGetCommandToMemcachedIfPossible()
    }

    /**
     * This Iteratee processes the responses from Memcached and sends messages back to
     * the IoActor whenever it has parsed a result
     */
    val iteratee = IO.IterateeRef.async(new Iteratees(self).processInput)(context.dispatcher)

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
        case found @ Found(key, value) => {
            poolActor ! found
            currentSet -= key
        }

        /**
         * A get command has finished. This will send the appropriate message
         * to the poolActor and make another command if necessary
         */
        case Finished => getCommandCompleted()
    }

}

/* Stores the result of a Memcached Get */
sealed trait GetResult {
    def key: String
}

/* Cache hit */
case class Found(key: String, value: ByteString) extends GetResult

/* Cache miss */
case class NotFound(key: String) extends GetResult
