package com.klout.akkamemcache

import akka.util.ByteString
import akka.actor._
import com.google.common.hash.Hashing._
import Protocol._
import ActorTypes._
import akka.actor.IO._

/**
 * Object sent to the IOActor indicating that a multiget request is complete.
 */
object Finished

/**
 * Objects of this class parse the output from Memcached and return
 * the cache hits and misses to the IoActor that manages the connection
 */
class Iteratees(ioActor: ActorRef) {

    def min(a: Int, b: Int) = {
        if (a < b) a else b
    }

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
            case End =>
                IO takeUntil CRLF map { _ =>
                    ioActor ! Finished
                    None
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
    val processValue = {
        for {
            whitespace <- IO takeUntil Space;
            key <- IO takeUntil Space;
            id <- IO takeUntil Space;
            length <- IO takeUntil CRLF map (ascii(_).toInt);
            value <- byteArray(length);
            newline <- IO takeUntil CRLF
        } yield {
            val found = Found(ascii(key), value)
            IO Done found
        }
    }

    /**
     * This iteratee generates a byte array result from Memcached. Because the
     * length of the memcached response is known, this has better performance
     * than bytestrings when deserializing.
     */
    def byteArray(length: Int): Iteratee[Array[Byte]] = {
        def continue(array: Array[Byte], total: Int, current: Int)(input: Input): (Iteratee[Array[Byte]], Input) = {
            input match {
                case Chunk(byteString) =>
                    val bytes = byteString.toArray
                    val numToCopy = min(total - current, bytes.size)

                    Array.copy(bytes, 0, array, current, numToCopy)

                    val chunk = if (numToCopy == bytes.size) {
                        Chunk.empty
                    } else {
                        Chunk(byteString drop numToCopy)
                    }

                    if (total == current + numToCopy) (Done(array), chunk)

                    else {
                        (Cont(continue(array, total, current + numToCopy)), chunk)
                    }

                case EOF(cause) => throw new Exception("EOF") //(Failure(new EOFException("Unexpected EOF")), EOF)
                case _          => throw new Exception("Something else")
            }
        }
        Cont(continue(new Array(length), length, 0))
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
            val instructions = keyValueMap map {
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
            val instructions = keys map {
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
        override def toByteString = {
            if (keys.size > 0) ByteString("get " + (keys mkString " ")) ++ CRLF
            else ByteString()
        }
    }

}
