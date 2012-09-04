package com.klout.akkamemcache

import akka.actor.IO
import akka.util.ByteString
import akka.actor._
import com.google.common.hash.Hashing._
import Protocol._
import ActorTypes._

/**
 * Object sent to the IOActor indicating that a multiget request is complete.
 */
object Finished

/**
 * Objects of this class parse the output from Memcached and return
 * the cache hits and misses to the IoActor that manages the connection
 */
class Iteratees(ioActor: ActorRef) {
    import Constants._

    /**
     * Skip over whitespace
     */
    def notWhitespace(byte: Byte): Boolean = {
        !whitespaceBytes.contains(byte)
    }

    val readInput = {
        (IO takeWhile notWhitespace) flatMap {

            /**
             * Cache hit
             */
            case Value => processValue

            /**
             * The cached values from a multiget have been returned
             */
            case End => {
                IO takeUntil CRLF map { _ =>
                    ioActor ! Finished
                    None
                }
            }

            case Error => IO takeUntil CRLF map (_ => None)

            case other => IO takeUntil CRLF map (_ => None)
        }
    }

    /**
     * Processes a cache hit from Memcached
     */
    val processValue = {
        for {
            whitespace <- IO takeUntil Space;
            key <- IO takeUntil Space;
            id <- IO takeUntil Space;
            length <- IO takeUntil CRLF map (ascii(_).toInt);
            value <- IO take length;
            newline <- IO takeUntil CRLF
        } yield {
            val found = Found(ascii(key), value)
            IO Done found
        }
    }

    /**
     * Consumes all of the input from the Iteratee and sends the results
     * to the appropriate IoActor.
     */
    val processInput = {
        IO repeat {
            readInput map {
                case IO.Done(found) => {
                    ioActor ! found
                }
                case _ => {}
            }
        }
    }

}

object Constants {

    val whitespace = List(' ', '\r', '\n', '\t')

    val whitespaceBytes = whitespace map (_.toByte)

    val Error = ByteString("ERROR")

    val Space = ByteString(" ")

    val CRLF = ByteString("\r\n")

    val Value = ByteString("VALUE")

    val End = ByteString("END")

}

object Protocol {
    import Constants._

    /**
     * Generates a human-readable ASCII representation of a ByteString
     */
    def ascii(bytes: ByteString): String = bytes.decodeString("US-ASCII").trim

    /**
     * This trait is for a command that the MemcachedClient will send to Memcached via an IoActor
     */
    trait Command {
        /**
         * Renders a ByteString that can be directly written to the connection
         * to a Memcached server
         */
        def toByteString: ByteString
    }

    case class SetCommand(keyValueMap: Map[String, ByteString], ttl: Long) extends Command {
        override def toByteString = {
            keyValueMap.map {
                case (key, value) =>
                    if (key.size == 0) throw new RuntimeException("A key is required")
                    if (!(key intersect whitespace).isEmpty)
                        throw new RuntimeException("Keys cannot have whitespace")
                    ByteString("set " + key + " 0 " + ttl + " " + value.size + " noreply") ++ CRLF ++ value ++ CRLF
            }.foldLeft(ByteString())(_ ++ _)
        }
    }

    case class DeleteCommand(keys: String*) extends Command {
        override def toByteString = {
            val command = keys.map {
                "delete " + _ + " noreply" + "\r\n"
            } mkString ""
            ByteString(command)
        }
    }

    case class GetCommand(keys: Set[String]) extends Command {
        override def toByteString = {
            if (keys.size > 0) ByteString("get " + (keys mkString " ")) ++ CRLF
            else ByteString()
        }
    }

}
