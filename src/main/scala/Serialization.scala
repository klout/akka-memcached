package com.klout.akkamemcache

import akka.util.ByteString
import org.jboss.serial.io._
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable
import java.io.IOException
import java.util.Calendar
import scala.collection.JavaConversions._
import org.apache.avro.util.ByteBufferInputStream

object Serialization {

    implicit def JBoss[T <: Any] = new SerializerWithDeserializer[T] {

        def using[C <: Closeable, V](closeables: C*)(f: () => V): V = {
            try {
                f.apply
            } finally {
                for (closeable <- closeables) { safely { closeable.close() } }
            }
        }

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
                        case other => {
                            println("Error: " + other)
                            throw other
                        }
                    }
            }
        }
        def deserialize(in: Array[Byte]): T = time("deserialize"){
            val bis = time("Create ByteArrayInputStream"){
                new ByteArrayInputStream(in)
            }
            val is = time("Create JBossObjectInputStream"){
                new JBossObjectInputStream(bis)
            }
            val obj = time("ReadObject"){
                using(bis, is) {
                    is readObject
                }
            }
            time("Cast object to correct type"){ obj.asInstanceOf[T] }
        }
    }
}

trait Serializer[T] {
    def serialize(t: T): ByteString
}

object Serializer {
    def serialize[T: Serializer](t: T): ByteString = implicitly[Serializer[T]] serialize t
}

trait Deserializer[T] {
    def deserialize(bytes: Array[Byte]): T
}

object Deserializer {
    def deserialize[T: Deserializer](bytes: Array[Byte]): T = implicitly[Deserializer[T]] deserialize bytes
}

trait SerializerWithDeserializer[T] extends Serializer[T] with Deserializer[T]