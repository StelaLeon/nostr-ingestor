package com.zoomin.earth.datalake.datapipelines

import cats.effect.IO
import munit.CatsEffectSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.zoomin.earth.datalake.datapipelines.orchestration.TimeWindowUpdateStrategy
import com.zoomin.earth.datalake.models.{NostrDataEvent, NostrFilter}
import com.zoomin.earth.datalake.models.NostrSubscription

class SubscriptionUpdateStrategyTest extends CatsEffectSuite {
  given logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  import com.zoomin.earth.datalake.nostr.NostrSerDesContext.given

  test("should update after 100 events") {
    val subscription = NostrSubscription[NostrFilter](
      id = "test-sub",
      filters = List(
        NostrFilter(authors = Some(List("author1")))
      )
    )
    val mockEvent = NostrDataEvent(
      id = "event-1",
      pubkey = "pubkey123",
      created_at = 1640000000,
      kind = 1,
      tags = List(List("e", "ref-event-id"), List("p", "ref-pubkey")),
      content = "Hello Nostr",
      sig = "sig123"
    )
    val strategy = new TimeWindowUpdateStrategy[NostrFilter](100, 7200, 1000L)
    val events   = List.fill(100)(mockEvent)
    val result   = strategy.shouldUpdate(events, subscription)
    assert(result.isDefined)
  }
}
