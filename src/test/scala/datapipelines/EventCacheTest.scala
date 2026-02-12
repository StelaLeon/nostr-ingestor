package com.zoomin.earth.datalake.datapipelines

import munit.CatsEffectSuite
import cats.effect.IO
import fs2.Stream

import scala.concurrent.duration.*
import munit.CatsEffectSuite
import cats.effect.IO
import cats.syntax.traverse.*
import cats.syntax.parallel.*
import com.zoomin.earth.datalake.datapipelines.processing.EventCache

class EventCacheTest extends CatsEffectSuite {
  test("get should return None for non-existent key") {
    for {
      cache  <- EventCache.make[IO, String, Int]
      result <- cache.get("missing")
    } yield assertEquals(result, None)
  }

  test("put and get should store and retrieve value") {
    for {
      cache  <- EventCache.make[IO, String, Int]
      _      <- cache.put("key1", 42)
      result <- cache.get("key1")
    } yield assertEquals(result, Some(42))
  }

  test("put should overwrite existing value") {
    for {
      cache  <- EventCache.make[IO, String, Int]
      _      <- cache.put("key1", 42)
      _      <- cache.put("key1", 100)
      result <- cache.get("key1")
    } yield assertEquals(result, Some(100))
  }

  test("put multiple keys and retrieve them") {
    for {
      cache   <- EventCache.make[IO, String, String]
      _       <- cache.put("key1", "value1")
      _       <- cache.put("key2", "value2")
      _       <- cache.put("key3", "value3")
      result1 <- cache.get("key1")
      result2 <- cache.get("key2")
      result3 <- cache.get("key3")
    } yield {
      assertEquals(result1, Some("value1"))
      assertEquals(result2, Some("value2"))
      assertEquals(result3, Some("value3"))
    }
  }

  test("remove should delete existing key") {
    for {
      cache  <- EventCache.make[IO, String, Int]
      _      <- cache.put("key1", 42)
      _      <- cache.remove("key1")
      result <- cache.get("key1")
    } yield assertEquals(result, None)
  }

  test("remove should be idempotent for non-existent key") {
    for {
      cache  <- EventCache.make[IO, String, Int]
      _      <- cache.remove("missing")
      result <- cache.get("missing")
    } yield assertEquals(result, None)
  }

  test("remove should not affect other keys") {
    for {
      cache   <- EventCache.make[IO, String, Int]
      _       <- cache.put("key1", 1)
      _       <- cache.put("key2", 2)
      _       <- cache.put("key3", 3)
      _       <- cache.remove("key2")
      result1 <- cache.get("key1")
      result2 <- cache.get("key2")
      result3 <- cache.get("key3")
    } yield {
      assertEquals(result1, Some(1))
      assertEquals(result2, None)
      assertEquals(result3, Some(3))
    }
  }

  test("clear should remove all entries") {
    for {
      cache   <- EventCache.make[IO, String, Int]
      _       <- cache.put("key1", 1)
      _       <- cache.put("key2", 2)
      _       <- cache.put("key3", 3)
      _       <- cache.clear
      result1 <- cache.get("key1")
      result2 <- cache.get("key2")
      result3 <- cache.get("key3")
    } yield {
      assertEquals(result1, None)
      assertEquals(result2, None)
      assertEquals(result3, None)
    }
  }

  test("clear should be idempotent") {
    for {
      cache  <- EventCache.make[IO, String, Int]
      _      <- cache.put("key1", 42)
      _      <- cache.clear
      _      <- cache.clear
      result <- cache.get("key1")
    } yield assertEquals(result, None)
  }

  test("cache should work with different types") {
    for {
      cache   <- EventCache.make[IO, Int, String]
      _       <- cache.put(1, "one")
      _       <- cache.put(2, "two")
      result1 <- cache.get(1)
      result2 <- cache.get(2)
    } yield {
      assertEquals(result1, Some("one"))
      assertEquals(result2, Some("two"))
    }
  }

  test("cache should work with complex types") {
    case class User(id: Int, name: String)

    for {
      cache <- EventCache.make[IO, String, User]
      user1 = User(1, "Alice")
      user2 = User(2, "Bob")
      _       <- cache.put("alice", user1)
      _       <- cache.put("bob", user2)
      result1 <- cache.get("alice")
      result2 <- cache.get("bob")
    } yield {
      assertEquals(result1, Some(user1))
      assertEquals(result2, Some(user2))
    }
  }

  test("concurrent puts should all be stored") {
    for {
      cache   <- EventCache.make[IO, Int, Int]
      _       <- List.range(0, 100).parTraverse(i => cache.put(i, i * 2))
      results <- List.range(0, 100).traverse(i => cache.get(i))
    } yield {
      assertEquals(results.flatten.size, 100)
      results.zipWithIndex.foreach { case (value, index) =>
        assertEquals(value, Some(index * 2))
      }
    }
  }

  test("concurrent operations should be consistent") {
    for {
      cache <- EventCache.make[IO, String, Int]
      _     <- cache.put("counter", 0)
      _     <- List
        .range(0, 50)
        .parTraverse(_ =>
          cache.get("counter").flatMap {
            case Some(v) => cache.put("counter", v + 1)
            case None    => cache.put("counter", 1)
          }
        )
      result <- cache.get("counter")
    } yield
      // Due to race conditions, final value might be less than 50
      assert(result.exists(_ > 0), s"Expected positive value, got $result")
  }

  test("put after clear should work") {
    for {
      cache   <- EventCache.make[IO, String, Int]
      _       <- cache.put("key1", 42)
      _       <- cache.clear
      _       <- cache.put("key2", 100)
      result1 <- cache.get("key1")
      result2 <- cache.get("key2")
    } yield {
      assertEquals(result1, None)
      assertEquals(result2, Some(100))
    }
  }

  test("operations on empty cache should work") {
    for {
      cache   <- EventCache.make[IO, String, Int]
      result1 <- cache.get("key")
      _       <- cache.remove("key")
      _       <- cache.clear
      result2 <- cache.get("key")
    } yield {
      assertEquals(result1, None)
      assertEquals(result2, None)
    }
  }

}
