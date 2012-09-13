package test

import akka.actor._
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.{ ByteString, Duration, Timeout }
import akka.util.duration._

import com.klout.akkamemcached._
import org.specs2.mutable.Specification
import scala.collection.mutable.HashMap
import scala.util.Random

object GiveMeTheState

class FakeIoActor extends Actor {
    val state: HashMap[String, Any] = new HashMap()
    def receive = {
        case Finished          => state += (("Finished" -> true))
        case GiveMeTheState    => sender ! state
        case key: String       => sender ! state.get(key)
        case kv: (String, Any) => state += kv
        case found: Found      => state += (found.key -> found.value)
    }
}

/**
 * This test verifies that the client understands the memcached protocol. It generates
 * fake outputs from memcached and ensures that the protocol correctly parses the
 * result.
 */
class MemcachedClientSpec extends Specification {
    implicit val timeout = Timeout(Duration("30 seconds")) // needed for `?` below
    implicit val system = ActorSystem()
    val fakeIoActor = system.actorOf(Props[FakeIoActor])
    val iteratee = IO.IterateeRef.sync(new Iteratees(fakeIoActor).processInput)

    sequential
    "The Iteratee" should {
        "send the Finished object to the IOActor when it recieves an END from Memcached" in {
            val bytes = ByteString("END\r\n")
            iteratee(IO Chunk bytes)
            Await.result((fakeIoActor ? "Finished"), Duration("1 second")).asInstanceOf[Option[Boolean]] must_== Some(true)
        }
        "properly parse a single result" in {
            val command = ByteString("VALUE testKey 0 10\r\nabcdefghij\r\nEND\r\n")
            iteratee(IO Chunk command)
            ByteString(Await.result((fakeIoActor ? "testKey"), Duration("1 second")).asInstanceOf[Option[Array[Byte]]].get) must_== ByteString("abcdefghij")
        }
        "properly parse multiple results" in {
            val command1 = ByteString("VALUE testKey2 0 5\r\n01234\r\n")
            val command2 = ByteString("VALUE testKey3 0 7\r\n5678910\r\nEND\r\n")
            val commands = command1 ++ command2
            iteratee(IO Chunk commands)
            ByteString(Await.result((fakeIoActor ? "testKey2"), Duration("1 second")).asInstanceOf[Option[Array[Byte]]].get) must_== ByteString("01234")
            ByteString(Await.result((fakeIoActor ? "testKey3"), Duration("1 second")).asInstanceOf[Option[Array[Byte]]].get) must_== ByteString("5678910")
        }
        "parse some very large results" in {
            val generator = new Random()
            var bytes1: Array[Byte] = new Array(10)
            var bytes2: Array[Byte] = new Array(50)
            Random.nextBytes(bytes1)
            Random.nextBytes(bytes2)
            val command1 = ByteString("VALUE random1 0 10\r\n") ++ ByteString(bytes1) ++ ByteString("\r\n")
            val command2 = ByteString("VALUE random2 0 50\r\n") ++ ByteString(bytes2) ++ ByteString("\r\nEND\r\n")
            val commands = command1 ++ command2
            iteratee(IO Chunk commands)
            ByteString(Await.result((fakeIoActor ? "random1"), Duration("1 second")).asInstanceOf[Option[Array[Byte]]].get) must_== ByteString(bytes1)
            ByteString(Await.result((fakeIoActor ? "random2"), Duration("1 second")).asInstanceOf[Option[Array[Byte]]].get) must_== ByteString(bytes2)
        }
        "parse a result that comes in two chunks" in {
            val part1 = ByteString("VALUE parts 0 10")
            val part2 = ByteString("\r\nabcdefghij\r\nEND\r\n")
            iteratee(IO Chunk part1)
            iteratee(IO Chunk part2)
            ByteString(Await.result((fakeIoActor ? "parts"), Duration("1 second")).asInstanceOf[Option[Array[Byte]]].get) must_== ByteString("abcdefghij")
        }
        "make sure that it has the valid information in the state" in {
            val state = Await.result((fakeIoActor ? GiveMeTheState), Duration("1 second")).asInstanceOf[HashMap[String, Any]]
            state.size must_== 7
            state.get("Finished") must_== Some(true)
            state.get("testKey") must beSome
            state.get("testKey2") must beSome
            state.get("testKey3") must beSome
            state.get("random1") must beSome
            state.get("random2") must beSome
            state.get("parts") must beSome
        }

    }
}