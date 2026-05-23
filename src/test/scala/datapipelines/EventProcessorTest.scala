package com.zoomin.earth.datalake.datapipelines

import cats.effect.IO
import cats.effect.std.Queue
import munit.CatsEffectSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.zoomin.earth.datalake.models.{NostrDataEvent, NostrFilter, NostrSubscription}
import com.zoomin.earth.datalake.datapipelines.orchestration.{SubscriptionUpdateStrategy, TimeWindowUpdateStrategy}
import com.zoomin.earth.datalake.datapipelines.processing.{
  EventProcessor,
  Process,
  EventCache,
  UpdateSubscription,
  ProcessingState
}

class EventProcessorTest extends CatsEffectSuite {
  given logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  def createEvent(id: String, createdAt: Long, pubkey: String = "author1"): NostrDataEvent =
    NostrDataEvent(
      id = id,
      pubkey = pubkey,
      created_at = createdAt,
      kind = 1,
      tags = List.empty,
      content = s"test content for $id",
      sig = s"sig_$id"
    )

  // Dummy handler for tests
  val dummyHandler: NostrSubscription[NostrFilter] => IO[Unit] =
    _ => IO.unit

  test("EventProcessor accumulates events when strategy returns None") {
    val event1 = createEvent("event1", 1000L)
    val event2 = createEvent("event2", 2000L)

    val subscription = NostrSubscription[NostrFilter](
      id = "test-sub",
      filters = List(
        NostrFilter(authors = Some(List("author1")))
      )
    )

    val neverUpdateStrategy = new SubscriptionUpdateStrategy[NostrFilter] {
      override def shouldUpdate(
        events: List[NostrDataEvent],
        subscription: NostrSubscription[NostrFilter]
      ): Option[NostrSubscription[NostrFilter]] = None
    }

    val action1 = EventProcessor.processEvent[IO, NostrFilter](
      event1,
      List.empty,
      subscription,
      neverUpdateStrategy,
      dummyHandler
    )

    action1 match {
      case Process(acc) => assertEquals(acc, List(event1))
      case _            => fail("Expected Process action")
    }

    val action2 = EventProcessor.processEvent[IO, NostrFilter](
      event2,
      List(event1),
      subscription,
      neverUpdateStrategy,
      dummyHandler
    )

    action2 match {
      case Process(acc) => assertEquals(acc, List(event1, event2))
      case _            => fail("Expected Process action")
    }
  }

  test("EventProcessor triggers update when strategy returns new subscription") {
    val event1 = createEvent("event1", 1000L)
    val event2 = createEvent("event2", 2000L)
    val event3 = createEvent("event3", 3000L)

    val subscription = NostrSubscription[NostrFilter](
      id = "test-sub",
      filters = List(
        NostrFilter(authors = Some(List("author1")), since = Some(1000L))
      )
    )

    val triggerAfter3Strategy = new SubscriptionUpdateStrategy[NostrFilter] {
      override def shouldUpdate(
        events: List[NostrDataEvent],
        subscription: NostrSubscription[NostrFilter]
      ): Option[NostrSubscription[NostrFilter]] =
        if events.size >= 3 then {
          val maxTimestamp = events.map(_.created_at).max
          Some(
            subscription.copy(
              filters = subscription.filters.map(f => f.copy(since = Some(maxTimestamp + 1)))
            )
          )
        } else None
    }

    val action1 = EventProcessor.processEvent[IO, NostrFilter](
      event1,
      List.empty,
      subscription,
      triggerAfter3Strategy,
      dummyHandler
    )
    action1 match {
      case Process(_) => // expected
      case _          => fail("Expected Process for first event")
    }

    val action2 = EventProcessor.processEvent[IO, NostrFilter](
      event2,
      List(event1),
      subscription,
      triggerAfter3Strategy,
      dummyHandler
    )
    action2 match {
      case Process(_) => // expected
      case _          => fail("Expected Process for second event")
    }

    val action3 = EventProcessor.processEvent[IO, NostrFilter](
      event3,
      List(event1, event2),
      subscription,
      triggerAfter3Strategy,
      dummyHandler
    )

    action3 match {
      case UpdateSubscription(newSub, handler) =>
        assertEquals(
          newSub.filters.head.since,
          Some(3001L),
          "New subscription should have updated 'since' timestamp"
        )
        // Verify the handler is the one we passed in
        assert(handler == dummyHandler, "Handler should be the same as passed")
      case _ => fail(s"Expected UpdateSubscription, got $action3")
    }
  }

  test("TimeWindowUpdateStrategy updates subscription on every non-empty batch") {
    val baseTime   = 1000L
    val windowSize = 3600L

    val strategy = new TimeWindowUpdateStrategy[NostrFilter](
      eventThreshold = 3,
      timeWindowSeconds = windowSize,
      originalStartTime = baseTime
    )

    val subscription = NostrSubscription[NostrFilter](
      id = "test-sub",
      filters = List(
        NostrFilter(authors = Some(List("author1")), since = Some(baseTime))
      )
    )

    val event1 = createEvent("e1", baseTime + windowSize + 500)
    val event2 = createEvent("e2", baseTime + windowSize + 600)

    val result1 = strategy.shouldUpdate(List(event1, event2), subscription)
    assert(result1.isDefined, "Should update on any non-empty batch")
    assert(
      result1.get.filters.head.since.exists(_ > baseTime),
      s"New 'since' should be greater than base time"
    )

    val event3 = createEvent("e3", baseTime + windowSize + 700)

    val result2 = strategy.shouldUpdate(List(event1, event2, event3), subscription)
    assert(result2.isDefined, "Should update with three events")
    assert(
      result2.get.filters.head.since.exists(_ > baseTime),
      s"New 'since' should be greater than base time"
    )
  }

  test("duplicate events are skipped based on cache") {
    for {
      cache <- EventCache.make[IO, String, Long]
      queue <- Queue.unbounded[IO, String]

      event1 = createEvent("event1", 1000L)

      cached1 <- cache.get(event1.id)
      _ = assertEquals(cached1, None)

      _ <- cache.put(event1.id, event1.created_at)

      cached2 <- cache.get(event1.id)
      _ = assertEquals(cached2, Some(event1.created_at))

      queueSize <- queue.size
      _ = assertEquals(queueSize, 0)

    } yield ()
  }

  test("ProcessingState tracks accumulator correctly") {
    val event1 = createEvent("e1", 1000L)
    val event2 = createEvent("e2", 2000L)

    val empty = ProcessingState.empty
    assertEquals(empty.accumulator, List.empty)

    val state1 = ProcessingState(List(event1))
    assertEquals(state1.accumulator.size, 1)

    val state2 = ProcessingState(List(event1, event2))
    assertEquals(state2.accumulator.size, 2)
    assertEquals(state2.accumulator.head.id, "e1")
    assertEquals(state2.accumulator.last.id, "e2")
  }
}
