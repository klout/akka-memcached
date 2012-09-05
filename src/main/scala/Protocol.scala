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

    val readInput = time("readInput"){
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
     * Each item sent by the server looks like this:
     *
     * VALUE <key> <flags> <bytes>\r\n
     * <data block>\r\n
     *
     */
    val processValue = time("processValue"){
        for {
            whitespace <- IO takeUntil Space;
            key <- IO takeUntil Space;
            id <- IO takeUntil Space;
            length <- IO takeUntil CRLF map (ascii(_).toInt);
            value <- IO take length;
            newline <- IO takeUntil CRLF
        } yield {
            time("Create found"){
                val found = Found(ascii(key), value)
                IO Done found
            }
        }
    }

    /**
     * Consumes all of the input from the Iteratee and sends the results
     * to the appropriate IoActor.
     */
    val processInput = time("processInput"){
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

    val CRLFString = "\r\n"

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

    /**
     * This command instructs Memcached to set multiple key-value pairs with a given ttl
     */
    case class SetCommand(keyValueMap: Map[String, ByteString], ttl: Long) extends Command {
        /**
         * Creates one memcached "set" instruction for each key-value pair, and concatenates the instructions
         * to be sent to the memcached server.
         *
         * A set instruction looks like:
         * set <key> <flags> <exptime> <bytes> [noreply]\r\n
         */
        override def toByteString = {
            val instructions = keyValueMap.map {
                case (key, value) =>
                    if (key.isEmpty) throw new RuntimeException("An empty string is not a valid key")
                    if (!(key intersect whitespace).isEmpty) throw new RuntimeException("Keys cannot have whitespace")

                    /* Single set instruction */
                    ByteString("set " + key + " 0 " + ttl + " " + value.size + " noreply") ++ CRLF ++ value ++ CRLF
            }

            /* Concatenated instructions */
            instructions.foldLeft(ByteString())(_ ++ _)
        }
    }

    /**
     * This commands instructs Memcached to delete one or more keys
     */
    case class DeleteCommand(keys: String*) extends Command {
        /**
         * Creates on memcached "delete" instruction for each key, and concatenates the instructions
         * to be sent to the memcached server.
         *
         * A delete instruction looks like:
         * delete <key> [noreply]\r\n
         */
        override def toByteString = {
            val instructions = keys.map {
                /* Single delete instruction */
                "delete " + _ + " noreply" + CRLFString
            }

            /* Concatenated instructions */
            ByteString(instructions mkString "")
        }
    }

    /**
     * This command instructs Memcached to get the value for one or more keys
     */
    case class GetCommand(keys: Set[String]) extends Command {
        /**
         * Creates a single Memcached multiget instruction to get all of the keys
         *
         * A get instruction looks like:
         * get <key>*\r\n
         */
        override def toByteString = time("getCommand toByteString"){
            if (keys.size > 0) ByteString("get " + (keys mkString " ")) ++ CRLF
            else ByteString()
        }
    }

}
