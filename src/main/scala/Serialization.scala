package com.klout.akkamemcache
import akka.util.ByteString
import org.jboss.serial.io._
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable
import java.io.IOException
import java.util.Calendar

trait Serializer[T] {
    def serialize(t: T): ByteString
}

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

object Serializer {

    def serialize[T: Serializer](t: T): ByteString = implicitly[Serializer[T]] serialize t

    implicit def any[T] = new Serializer[T] {
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
    }
}

trait Deserializer[T] {
    def deserialize(bytes: ByteString): T
}

object Deserializer {

    implicit def any[T] = new Deserializer[T] {
        def deserialize(in: ByteString): T = {
            val bis = new ByteArrayInputStream(in.iterator.toArray)
            val is = new JBossObjectInputStream(bis)
            val obj = using(bis, is) {
                is readObject
            }
            obj.asInstanceOf[T]
        }
    }

    def deserialize[T: Deserializer](bytes: ByteString): T = implicitly[Deserializer[T]] deserialize bytes
}