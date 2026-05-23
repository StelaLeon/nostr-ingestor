package com.zoomin.earth.datalake.datapipelines.mocks

import com.zoomin.earth.datalake.backends.websocket.WebSocketClient
import cats.effect.{IO, Ref}
import cats.effect.kernel.{Concurrent, Temporal}
import cats.effect.std.Queue
import cats.effect.unsafe.implicits.global
import com.zoomin.earth.datalake.db.DBClient
import com.zoomin.earth.datalake.models.*
import com.zoomin.earth.datalake.datapipelines.orchestration.{SubscriptionUpdateStrategy, TimeWindowUpdateStrategy}
import com.zoomin.earth.datalake.datapipelines.DataPipeline
import com.zoomin.earth.datalake.datapipelines.mocks.DataPipelineTestFixture.{MockOutput, mockTransformFunction}
import io.circe.Encoder
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.typelevel.log4cats.Logger
import sttp.capabilities.WebSockets
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.testing.SttpBackendStub
import sttp.client3.{Response, SttpBackend}
import sttp.model.{Headers, RequestMetadata, ResponseMetadata, StatusCode}
import sttp.monad.MonadError
import cats.syntax.traverse.*
import com.zoomin.earth.datalake.nostr.NostrSerDesContext
import munit.Clue.generate
import com.zoomin.earth.datalake.parser.instances.given
import com.zoomin.earth.datalake.config.PipelineConfig

class MockDataPipeline[T <: NostrFilterBase, O](
  testSink: DBClient[O, IO],
  backend: WebSocketClient[IO],
  f: (NostrDataEvent, String) => O,
  config: PipelineConfig,
  updateStrategy: SubscriptionUpdateStrategy[T] =
    new TimeWindowUpdateStrategy[T](eventThreshold = 3, timeWindowSeconds = 3600L, originalStartTime = 1000L)
)(using Concurrent[IO], Temporal[IO], Logger[IO], com.zoomin.earth.datalake.parser.Parser[T])
  extends DataPipeline[T, O, IO](testSink, backend, f, config, updateStrategy)

object DataPipelineTestFixture {

  case class MockOutput(
    id: String,
    relay: String,
    content: String,
    kind: Int,
    pubkey: String
  )

  def createTestEventStream(relayId: String): List[NostrEvent] = List(
    NostrDataEvent(
      id = "event-1",
      pubkey = "pubkey123",
      created_at = 1640000000,
      kind = 1,
      tags = List(List("e", "ref-event-id"), List("p", "ref-pubkey")),
      content = "Hello Nostr",
      sig = "sig123"
    ),
    NostrDataEvent(
      id = "event-2",
      pubkey = "pubkey456",
      created_at = 1640000100,
      kind = 1,
      tags = List.empty,
      content = "Test message",
      sig = "sig456"
    ),
    NostrDataEvent(
      id = "event-3",
      pubkey = "pubkey789",
      created_at = 1640000200,
      kind = 7,
      tags = List(List("e", "liked-event-id")),
      content = "+",
      sig = "sig789"
    ),
    EOSE(relayId) // Signal to stop the fiber
  )

  def mockTransformFunction(event: NostrDataEvent, relay: String): MockOutput =
    MockOutput(event.id, relay, event.content, event.kind, event.pubkey)

  def createMockBackend(
    events: Map[String, List[NostrDataEvent]]
  )(using Concurrent[IO]): IO[WebSocketClient[IO]] = {

    import NostrSerDesContext.given
    MockWebSocketClient.withEvents(events.head._2.map(_.asJson))
  }

  def createTestPipeline(
    testSink: TestDBClient[MockOutput, IO],
    mockBackend: WebSocketClient[IO],
    config: PipelineConfig
  )(using
    Concurrent[IO],
    Temporal[IO],
    Logger[IO]
  ): MockDataPipeline[NostrFilter, MockOutput] =
    new MockDataPipeline[NostrFilter, MockOutput](
      testSink,
      mockBackend,
      mockTransformFunction,
      config
    )

  def createTestSink[MockOutput](): IO[TestDBClient[MockOutput, IO]] =
    TestDBClient[MockOutput, IO]
}
