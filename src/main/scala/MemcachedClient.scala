package com.klout.akkamemcache

import akka.dispatch.Future
import akka.actor._
import akka.util.Duration
import akka.util.duration._
import akka.pattern.ask
import akka.util.Timeout
import akka.util.ByteString

import com.klout.akkamemcache.Protocol._

trait MemcachedClient {

    val DefaultDuration = 1 hour

    def set[T: Serializer](key: String, value: T, ttl: Duration = DefaultDuration): Unit

    def mset[T: Serializer](values: Map[String, T], ttl: Duration = DefaultDuration): Unit

    def get[T: Deserializer](key: String): Future[Option[T]]

    def mget[T: Deserializer](keys: Set[String]): Future[Map[String, T]]

    def delete(keys: String*): Unit

}

/**
 * Asynchronous memcached client.
 */
class RealMemcachedClient(hosts: List[(String, Int)]) extends MemcachedClient {
    implicit val timeout = Timeout(5 seconds) // needed for `?` below

    val system = ActorSystem()

    val poolActor = system.actorOf(Props(new PoolActor(hosts)), name = "PoolActor")

    override def set[T: Serializer](key: String, value: T, ttl: Duration) {
        mset(Map(key -> value), ttl)
    }

    override def mset[T: Serializer](keyValueMap: Map[String, T], ttl: Duration) {
        val serializedKeyValueMap = keyValueMap map {
            case (key, value) => key -> Serializer.serialize(value)
        }
        poolActor ! SetCommand(serializedKeyValueMap, ttl.toSeconds)
    }

    override def get[T: Deserializer](key: String): Future[Option[T]] = {
        mget(Set(key)).map(_.get(key))
    }

    override def mget[T: Deserializer](keys: Set[String]): Future[Map[String, T]] = {
        val command = GetCommand(keys)
        (poolActor ? command).map{
            case result: List[GetResult] => {
                result.flatMap {
                    case Found(key, value) => Some(key, Deserializer.deserialize[T](value))
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