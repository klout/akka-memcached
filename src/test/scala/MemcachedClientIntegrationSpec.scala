package test

import akka.dispatch.Await
import akka.util.Duration
import akka.util.duration._
import com.klout.akkamemcache._
import org.specs2.mutable.Specification
import Serialization.JBoss

case class TestClass(test1: String,
                     test2: Map[String, Int])


/**
 * This tests that set, get, and delete funciton properly on a local memcached conneciton.
 * You need to run a local memcached server in order for this test to function properly.
 */
class MemcachedClientIntegrationSpec extends Specification {

    val client = new RealMemcachedClient(List(("localhost", 11211)), 1)
    val noTTL = Duration("0 seconds")
    val timeout = Duration("5 seconds")

    sequential
    "The memcached client" should {
        "set some values, and get them later" in {
            client.set("key1", "value1", noTTL)
            client.set("key2", "value2", noTTL)
            Thread.sleep(50)

            val value1 = Await.result(client.get[String]("key1"), timeout)
            val value2 = Await.result(client.get[String]("key2"), timeout)
            value1 must_== Some("value1")
            value2 must_== Some("value2")
        }
        "use multiget to get values, and miss nonexistent values" in {
            val valueMap = Await.result(client.mget(Set("key1", "key2", "key3")), timeout)

            valueMap.get("key1") must_== Some("value1")
            valueMap.get("key2") must_== Some("value2")
            valueMap.get("key3") must beNone
        }
        "change existing values" in {
            client.set("key1", "value3", noTTL)
            client.set("key2", "value4", noTTL)
            Thread.sleep(50)

            val valueMap = Await.result(client.mget(Set("key1", "key2")), timeout)
            valueMap.get("key1") must_== Some("value3")
            valueMap.get("key2") must_== Some("value4")
        }
        "delete a value" in {
            client.delete("key1")
            Thread.sleep(50)

            val value1 = Await.result(client.get("key1"), timeout)
            value1 must beNone
        }
        "set multiple values" in {
            client.mset(Map("key4" -> "value4", "key5" -> "value5"), noTTL)
            Thread.sleep(50)

            val valueMap = Await.result(client.mget(Set("key4", "key5")), timeout)
            valueMap.get("key4") must_== Some("value4")
            valueMap.get("key5") must_== Some("value5")
        }
        "delete multiple values" in {
            client.delete("key4", "key5")
            Thread.sleep(50)

            val valueMap = Await.result(client.mget(Set("key4", "key5")), timeout)
            valueMap.get("key4") must beNone
            valueMap.get("key5") must beNone
        }
        "use valid TTLs" in {
            client.set("key6", "value6", Duration("2 seconds"))
            Thread.sleep(1000)

            val value6 = Await.result(client.get("key6"), timeout)
            value6 must_== Some("value6")
            Thread.sleep(1200)

            val value6None = Await.result(client.get("key6"), timeout)
            value6None must beNone
        }
        "handle many multigets,multisets, and multideletes" in {
            (0 to 9).foreach { iteration =>
                val prefix = iteration.toString + "."
                val keys: IndexedSeq[String] = (1 to 1000).map(prefix + _.toString)
                val sets = keys.map{ key => key -> (key + ".value") }.toMap
                client.mset(sets, noTTL)
                Thread.sleep(50)

                val results = Await.result(client.mget[String](keys.toSet), timeout)
                results.forall {
                    case (key, value) => value must_== key + ".value"
                }
                client.delete(keys: _*)
            }
        }
        "handle many individual gets, sets, and deletes" in {
            (10 to 19).foreach { iteration =>
                val prefix = iteration.toString + "."
                val keys = (1 to 10).map(prefix + _.toString)
                val sets = keys.map{ key => key -> (key + ".value") }.toMap
                sets foreach {
                    case (key, value) => client set (key, value, timeout)
                }
                Thread.sleep(50)

                val results = keys map {
                    key => (key, Await.result(client.get[String](key), timeout))
                }
                results foreach {
                    case (key, Some(value)) => value must_== key + ".value"
                    case (key, None)        => key must_== "could not be found"
                }
                keys foreach {
                    key => client.delete(key)
                }
                Thread.sleep(50)

                val deletedResults = Await.result(client.mget[String](keys.toSet), timeout)
                deletedResults.values forall (_ == None) must beTrue
            }
        }
        "serialize and deserialize maps and lists" in {
            val map = ('a' to 'z').map{
                letter => letter -> letter.toInt
            }.toMap
            val list = 1 to 100
            client.mset(Map("map1" -> map, "list1" -> list), noTTL)
            Thread.sleep(50)

            val results = Await.result(client.mget(Set("map1", "list1")), timeout)
            results.get("map1") must_== Some(map)
            results.get("list1") must_== Some(list)
        }
        "serialize case classes" in {
            val testObject = TestClass("Foo", Map("Bar1" -> 1, "Bar2" -> 2))
            client.set("testObject", testObject, noTTL)
            Thread.sleep(50)

            val result = Await.result(client.get("testObject"), timeout)
            result must_== Some(testObject)
        }
        "delete all of the keys used in this test" in {
            val keys = List("key1", "key2", "key3", "key4", "key5", "key6", "map1", "list1", "testObject")
            client.delete(keys: _*)
            Thread.sleep(50)

            val keyValueMap = Await.result(client.mget[Object](keys toSet), timeout)
            keyValueMap.values forall (_ == None) must beTrue
        }

    }
}