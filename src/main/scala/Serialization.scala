package com.klout.akkamemcache

import akka.util.ByteString
import org.jboss.serial.io._
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable
import java.io.IOException
import java.util.Calendar
import scala.collection.JavaConversions._

/**
 * This object contains serializers that can be used by the Memcached client
 * to serialize and deserialize data.
 */
object Serialization {

    /**
     * Import this serializer to use JBoss serialization with Memcached
     */
    implicit def JBoss[T <: Any] = new Serializer[T] {

        /**
         * Uses a connection or stream to compute a result, and closes
         * the connection once the result is computed
         */
        def using[C <: Closeable, V](closeables: C*)(f: () => V): V = {
            try {
                f.apply
            } finally {
                for (closeable <- closeables) { safely { closeable.close() } }
            }
        }

        /**
         * Swallows any exception
         */
        def safely(f: => Any) {
            try { f } catch { case error => {} }
        }

        def serialize(o: T): ByteString = {
            Option(o) match {
                case None => throw new NullPointerException("Can't serialize null")
                case Some(o) =>
                    try {
                        val bos = new ByteArrayOutputStream
                        val os = new JBossObjectOutputStream(bos)

                        val byteArray = using (bos, os) {
                            os writeObject o
                            bos.toByteArray
                        }
                        ByteString(byteArray)
                    } catch {
                        case e: IOException => throw new IllegalArgumentException("Non-serializable object", e);
                    }
            }
        }

        def deserialize(in: Array[Byte]): T = {
            val bis = new ByteArrayInputStream(in)
            val is = new JBossObjectInputStream(bis)
            val obj = using(bis, is) {
                is readObject
            }
            obj.asInstanceOf[T]
        }
    }
}

/**
 * Helper object used by RealMemcachedClient for serialization. Requires an implicit
 * serializer within it's scope
 */
object Serializer {
    def serialize[T: Serializer](t: T): ByteString = implicitly[Serializer[T]] serialize t
    def deserialize[T: Serializer](bytes: Array[Byte]): T = implicitly[Serializer[T]] deserialize bytes
}

/**
 * You can define your own serializer by mixing in this trait
 */
trait Serializer[T] {

    /**
     * Converts an object of type T into an akka.util.ByteString for storage
     * into Memcached.
     */
    def serialize(t: T): ByteString

    /**
     * Converts a bytearray from Memcached into an object of type T
     */
    def deserialize(bytes: Array[Byte]): T
}