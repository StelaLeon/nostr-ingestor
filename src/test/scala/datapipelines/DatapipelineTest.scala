package com.zoomin.earth.datalake.backends

import cats.effect.IO
import com.zoomin.earth.datalake.models.{
  BigQueryNostrAuthoredEvent,
  NostrDataEvent,
  NostrFilterAuthored,
  NostrFilterUnauthored,
  NostrSubscription
}
import com.zoomin.earth.datalake.nostr.NostrOps.*
import com.zoomin.earth.datalake.datapipelines.DataPipeline
import com.zoomin.earth.datalake.datapipelines.mocks.{DataPipelineTestFixture, MockWebSocketClient}
import com.zoomin.earth.datalake.config.ConfigLoader
import io.circe.syntax.*
import io.circe.{Encoder, Json}
import munit.CatsEffectSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.zoomin.earth.datalake.datapipelines.orchestration.TimeWindowUpdateStrategy

import java.time.Instant
import scala.concurrent.duration.*

class DatapipelineTest extends CatsEffectSuite {
  given logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  import com.zoomin.earth.datalake.nostr.NostrSerDesContext.given
  import com.zoomin.earth.datalake.parser.instances.given

  test("get should return None for non-existent key") {
    val events = Map(
      "wss://relay1.nostr" -> List(
        Json.obj(
          "id"         -> "event1".asJson,
          "pubkey"     -> "author1".asJson,
          "created_at" -> 1234567890.asJson,
          "kind"       -> 1.asJson,
          "content"    -> "Hello".asJson
        ),
        Json.obj(
          "id"         -> "event2".asJson,
          "pubkey"     -> "author2".asJson,
          "created_at" -> 1234567900.asJson,
          "kind"       -> 1.asJson,
          "content"    -> "World".asJson
        )
      ),
      "wss://relay2.nostr" -> List()
    )

    val allEvents = Map(
      "wss://relay.nostr" -> events.map(_.asJson)
    )

    val subscription = NostrSubscription[NostrFilterUnauthored](
      id = "test-sub",
      filters = List(
        NostrFilterUnauthored(authors = Some(List("nonexistent-author")))
      )
    )

    for {
      mockedWs   <- MockWebSocketClient.withFilteredEvents(events)
      mockedSink <- DataPipelineTestFixture.createTestSink[BigQueryNostrAuthoredEvent]()
      config     <- ConfigLoader.load[IO]

      subscriptionUpdateStrategy = TimeWindowUpdateStrategy[NostrFilterUnauthored](originalStartTime =
        config.relays.syncSince
      )

      pipeline = new DataPipeline[NostrFilterUnauthored, BigQueryNostrAuthoredEvent, IO](
        mockedSink,
        mockedWs,
        (ev: NostrDataEvent, r: String) => ev.toNostrAuthored(r),
        config,
        subscriptionUpdateStrategy
      ) {}
      _ <- pipeline.createPipeline(List("wss://relay.nostr"), subscription).use { stream =>
        stream.compile.drain.timeout(5.seconds)
      }
      results <- mockedSink.getSavedItems
    } yield assertEquals(results.size, 0)
  }

  test("get should return the list of authors for an existent key") {
    val events = Map(
      "wss://relay1.nostr" -> List(
        Json.obj(
          "id"         -> "event1".asJson,
          "pubkey"     -> "author1".asJson,
          "created_at" -> 1234567890.asJson,
          "kind"       -> 1.asJson,
          "content"    -> "Hello".asJson
        ),
        Json.obj(
          "id"         -> "event2".asJson,
          "pubkey"     -> "author2".asJson,
          "created_at" -> 1234567900.asJson,
          "kind"       -> 1.asJson,
          "content"    -> "World".asJson
        )
      ),
      "wss://relay2.nostr" -> List()
    )

    val allEvents = Map(
      "wss://relay.nostr" -> events.map(_.asJson)
    )

    val subscription = NostrSubscription[NostrFilterUnauthored](
      id = "test-sub",
      filters = List(
        NostrFilterUnauthored(authors = Some(List("author1")))
      )
    )

    for {
      mockedWs   <- MockWebSocketClient.withFilteredEvents(events)
      mockedSink <- DataPipelineTestFixture.createTestSink[BigQueryNostrAuthoredEvent]()
      config     <- ConfigLoader.load[IO]

      subscriptionUpdateStrategy = TimeWindowUpdateStrategy[NostrFilterUnauthored](originalStartTime =
        config.relays.syncSince
      )

      pipeline = new DataPipeline[NostrFilterUnauthored, BigQueryNostrAuthoredEvent, IO](
        mockedSink,
        mockedWs,
        (ev: NostrDataEvent, r: String) => ev.toNostrAuthored(r),
        config,
        subscriptionUpdateStrategy
      ) {}
      _ <- pipeline.createPipeline(List("wss://relay.nostr"), subscription).use { stream =>
        stream.compile.drain.timeout(5.seconds)
      }
      results <- mockedSink.getSavedItems
    } yield assertEquals(results.size, 0)
  }
}
