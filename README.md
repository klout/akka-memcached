akka-memcached
==============

async, non-blocking memcached client using akka

inspired by, but much simpler than, this project: <https://github.com/derekjw/fyrie-redis>

info on akka io / iteratee library: <http://doc.akka.io/docs/akka/2.0.3/scala/io.html>


Features
--------
 * Supports set, get, and delete memcached instructions
 * All instructions are non-blocking.
 * The set and delete methods are fire-and-forget, the get method returns an `akka.dispatch.Future`
 * Uses consistent hashing to distribute data across multiple memcached servers
 * Deduplicates keys: If multiple clients make a request for the same key, only a single get request will be made for that key, and results will be forwarded to each requesting client


How to build
------------
 * Requires sbt https://github.com/harrah/xsbt/wiki
 * cd into the akka-memcached directory and run `sbt package`
 * The jar will be located in target/scala-2.9.1

Usage
-----
 * Include this jar in your project
 * Create a new instance of RealMemcachedClient
 * It is necessary to have an implicit serializer when calling the get or set client methods

Example
-------

	import akka.util.duration._
	import akka.util.Timeout
	import akka.dispatch.Await
	import com.klout.akkamemcache.RealMemcachedClient
	import com.klout.akkamemcache.Serialization.JBoss

	object Test {
		// Create a client that connects to localhost on a single connection
	    val client = new RealMemcachedClient(List(("localhost", 11211)), 1)

	    // Store a value
	    client.set("key", "value", 1 hour)

	    // Retrieve a value
	    val valueFuture = client.get("key")
	    val result = Await.result(valueFuture, 5 seconds)

	    // Delete a value
	    client.delete("key")
	}
