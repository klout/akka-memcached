package com.klout.akkamemcache

import akka.dispatch.Future
import akka.actor._
import akka.util.Duration
import akka.util.duration._
import akka.pattern.ask
import akka.util.Timeout
import akka.util.ByteString
import java.util.Calendar
import java.net.URLEncoder._

import com.klout.akkamemcache.Protocol._

/**
 * Asynchronous memcached client.
 */
trait MemcachedClient {

    val DefaultTTL = 1 hour

    /**
     * Sets a single key - Fire and Forget
     */
    def set[T: Serializer](key: String, value: T, ttl: Duration = DefaultTTL): Unit

    /**
     * Sets multiple key-value pairs, all with the same TTL - Fire and Forget
     */
    def mset[T: Serializer](values: Map[String, T], ttl: Duration = DefaultTTL): Unit

    /**
     * Retrieves the value of a single key. In the case of a cache miss, this method will
     * return a Future containing None. Otherwise, this method will return a Future of
     * Some[T]
     */
    def get[T: Serializer](key: String): Future[Option[T]]

    /**
     * Retrieves the values of multiple keys. This method returns a future of a mapping from
     * cache keys to values. Keys that do not exist in Memcached will not be included in the
     * map
     */
    def mget[T: Serializer](keys: Set[String]): Future[Map[String, T]]

    /**
     * Deletes multiple keys - Fire and Forget
     */
    def delete(keys: String*): Unit

}

class RealMemcachedClient(hosts: List[(String, Int)], connectionsPerServer: Int = 1) extends MemcachedClient {

    /**
     * Maximum amount of time the client will wait for a response from
     * a get instruction from Memcached
     */
    implicit val timeout = Timeout(60 seconds)

    val system = ActorSystem()

    val poolActor = system.actorOf(Props(new PoolActor(hosts, connectionsPerServer)), name = encode("Pool Actor"))

    override def set[T: Serializer](key: String, value: T, ttl: Duration) {
        mset(Map(key -> value), ttl)
    }

    override def mset[T: Serializer](keyValueMap: Map[String, T], ttl: Duration) {
        val serializedKeyValueMap = keyValueMap map {
            case (key, value) => key -> Serializer.serialize(value)
        }
        poolActor ! SetCommand(serializedKeyValueMap, ttl.toSeconds)
    }

    override def get[T: Serializer](key: String): Future[Option[T]] = {
        mget(Set(key)).map(_.get(key))
    }

    override def mget[T: Serializer](keys: Set[String]): Future[Map[String, T]] = {
        val command = GetCommand(keys)
        (poolActor ? command).map{
            case result: List[GetResult] => {
                result.flatMap {
                    case Found(key, value) => Some((key, Serializer.deserialize[T](value)))
                    case NotFound(key)     => None
                }
            }.toMap
            case other => throw new Exception("Invalid result returned: " + other)
        }
    }

    override def delete(keys: String*) {
        poolActor ! DeleteCommand(keys: _*)
    }

}