package com.klout.akkamemcache
import akka.util.ByteString
import org.jboss.serial.io._
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable
import java.io.IOException
import java.util.Calendar
import scala.collection.JavaConversions._

object `package` {

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
}

object Serialization {
    implicit def JBoss[T <: Any] = new SerializerWithDeserializer[T] {
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
        def deserialize(in: ByteString): T = {
            //val start = Calendar.getInstance().getTimeInMillis
            val bis = new ByteArrayInputStream(in.toArray)
            val is = new JBossObjectInputStream(bis)
            val obj = using(bis, is) {
                is readObject
            }
            val result = obj.asInstanceOf[T]
            //val end = Calendar.getInstance().getTimeInMillis
            //println("Deserialization took: " + (end - start) + " ms")
            result
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
    def deserialize(bytes: ByteString): T
}

object Deserializer {
    def deserialize[T: Deserializer](bytes: ByteString): T = implicitly[Deserializer[T]] deserialize bytes
}

trait SerializerWithDeserializer[T] extends Serializer[T] with Deserializer[T]