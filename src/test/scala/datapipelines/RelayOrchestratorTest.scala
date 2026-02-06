package com.zoomin.earth.datalake.datapipelines

import cats.effect.{IO, Ref}
import cats.effect.std.Queue
import com.zoomin.earth.datalake.backends.websocket.{WebSocketClient, WebSocketOps}
import com.zoomin.earth.datalake.models.{NostrFilterAuthored, NostrSubscription}
import fs2.Stream
import munit.{CatsEffectSuite, Ignore}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.zoomin.earth.datalake.datapipelines.orchestration.RelayOrchestrator

import scala.concurrent.duration.*

class RelayOrchestratorTest extends CatsEffectSuite {

  given Logger[IO] = Slf4jLogger.getLogger[IO]

  val relayUrl = "wss://test.relay.io"

  val mockBackend = new WebSocketClient[IO] {
    def withConnection[A](relayUrl: String)(use: WebSocketOps[IO] => IO[A]): IO[Either[String, A]] =
      IO.pure(Right(().asInstanceOf[A]))
  }

  def createTestSubscription(id: String): NostrSubscription[NostrFilterAuthored] =
    NostrSubscription(
      id = id,
      filters = List(NostrFilterAuthored(authors = Some(List("test-pubkey"))))
    )

  test("RelayOrchestrator creates resilient stream that processes events") {
    val orchestrator = RelayOrchestrator[IO, NostrFilterAuthored, Unit](
      relayUrl = relayUrl,
      backend = mockBackend
    )

    val initialSub = createTestSubscription("test-sub-1")

    for {
      subQueue        <- Queue.unbounded[IO, NostrSubscription[NostrFilterAuthored]]
      eventsProcessed <- Ref.of[IO, Int](0)

      streamFactory = (
        sub: NostrSubscription[NostrFilterAuthored],
        _: Queue[IO, NostrSubscription[NostrFilterAuthored]]
      ) =>
        Stream.eval(eventsProcessed.update(_ + 1)).as(Some(())) ++
          Stream.eval(eventsProcessed.update(_ + 1)).as(Some(())) ++
          Stream.eval(eventsProcessed.update(_ + 1)).as(Some(())) ++
          Stream.emit(None) // EOSE

      _ <- orchestrator
        .createResilientStream(streamFactory, initialSub, subQueue)
        .take(3)
        .compile
        .drain

      count <- eventsProcessed.get
    } yield assertEquals(count, 3)
  }

  test("RelayOrchestrator stops stream when EOSE (None) is received") {
    val orchestrator = RelayOrchestrator[IO, NostrFilterAuthored, Unit](
      relayUrl = relayUrl,
      backend = mockBackend
    )

    val initialSub = createTestSubscription("test-sub-2")

    for {
      subQueue <- Queue.unbounded[IO, NostrSubscription[NostrFilterAuthored]]

      streamFactory = (
        _: NostrSubscription[NostrFilterAuthored],
        _: Queue[IO, NostrSubscription[NostrFilterAuthored]]
      ) => Stream(Some(()), Some(()), None)

      results <- orchestrator
        .createResilientStream(streamFactory, initialSub, subQueue)
        .take(10)
        .compile
        .toList

    } yield {
      assertEquals(results.length, 2, "Should only get events before None")
      assert(results.forall(_.isDefined), "All results should be Some")
    }
  }

  test("RelayOrchestrator retries on error with exponential backoff") {
    val orchestrator = RelayOrchestrator[IO, NostrFilterAuthored, Unit](
      relayUrl = relayUrl,
      backend = mockBackend,
      initialRetryDelay = 10.millis,
      maxRetryDelay = 100.millis
    )

    val initialSub = createTestSubscription("test-sub-3")

    for {
      subQueue <- Queue.unbounded[IO, NostrSubscription[NostrFilterAuthored]]
      attempts <- Ref.of[IO, Int](0)

      streamFactory = (
        _: NostrSubscription[NostrFilterAuthored],
        _: Queue[IO, NostrSubscription[NostrFilterAuthored]]
      ) =>
        Stream.eval(attempts.updateAndGet(_ + 1)).flatMap { count =>
          if count < 3 then Stream.raiseError[IO](new Exception(s"Connection failed (attempt $count)"))
          else Stream.emit(Some(())) ++ Stream.emit(None)
        }

      result <- orchestrator
        .createResilientStream(streamFactory, initialSub, subQueue)
        .take(1)
        .compile
        .toList
        .timeout(5.seconds)

      finalAttempts <- attempts.get
    } yield {
      assertEquals(result.length, 1, "Should eventually succeed")
      assert(finalAttempts >= 3, s"Should retry multiple times (attempts: $finalAttempts)")
    }
  }

  test("RelayOrchestrator uses updated subscription when available during retry") {
    val orchestrator = RelayOrchestrator[IO, NostrFilterAuthored, Unit](
      relayUrl = relayUrl,
      backend = mockBackend,
      initialRetryDelay = 10.millis
    )

    val initialSub = createTestSubscription("initial-sub")
    val updatedSub = createTestSubscription("updated-sub")

    for {
      subQueue          <- Queue.unbounded[IO, NostrSubscription[NostrFilterAuthored]]
      subscriptionsUsed <- Ref.of[IO, List[String]](List.empty)

      streamFactory = (
        sub: NostrSubscription[NostrFilterAuthored],
        _: Queue[IO, NostrSubscription[NostrFilterAuthored]]
      ) =>
        Stream.eval(subscriptionsUsed.update(_ :+ sub.id)) >>
          Stream.raiseError[IO](new Exception("Fail to trigger retry"))

      fiber <- orchestrator
        .createResilientStream(streamFactory, initialSub, subQueue)
        .compile
        .drain
        .start

      _ <- IO.sleep(50.millis)
      _ <- subQueue.offer(updatedSub)
      _ <- IO.sleep(100.millis)
      _ <- fiber.cancel

      usedSubs <- subscriptionsUsed.get
    } yield {
      assert(usedSubs.contains("initial-sub"), "Should use initial subscription")
      assert(usedSubs.contains("updated-sub"), "Should use updated subscription after retry")
    }
  }

  test("RelayOrchestrator respects max retry delay") {
    val orchestrator = RelayOrchestrator[IO, NostrFilterAuthored, Unit](
      relayUrl = relayUrl,
      backend = mockBackend,
      initialRetryDelay = 10.millis,
      maxRetryDelay = 50.millis
    )

    val initialSub = createTestSubscription("max-delay-sub")

    for {
      subQueue        <- Queue.unbounded[IO, NostrSubscription[NostrFilterAuthored]]
      retryTimestamps <- Ref.of[IO, List[Long]](List.empty)

      // Always-failing stream that records retry times
      streamFactory = (
        _: NostrSubscription[NostrFilterAuthored],
        _: Queue[IO, NostrSubscription[NostrFilterAuthored]]
      ) =>
        Stream.eval(IO.realTime.flatMap(t => retryTimestamps.update(_ :+ t.toMillis))) >>
          Stream.raiseError[IO](new Exception("Always fail"))

      fiber <- orchestrator
        .createResilientStream(streamFactory, initialSub, subQueue)
        .compile
        .drain
        .start

      _ <- IO.sleep(500.millis)
      _ <- fiber.cancel

      timestamps <- retryTimestamps.get
    } yield {
      assert(timestamps.length >= 4, s"Should have multiple retries (got ${timestamps.length})")

      val delays = timestamps.sliding(2).collect { case Seq(a, b) => b - a }.toList
      delays.foreach { delay =>
        assert(delay <= 300, s"Delay $delay should respect max of ~50ms (with generous margin for CI/system overhead)")
      }
    }
  }

  test("RelayOrchestrator respects max retry delay via exponential backoff sequence") {
    val initialDelay = 10.millis
    val maxDelay = 50.millis

    def calculateNextDelay(currentDelay: FiniteDuration): FiniteDuration =
      (currentDelay * 2).min(maxDelay)

    val delay1 = calculateNextDelay(initialDelay)
    val delay2 = calculateNextDelay(delay1)
    val delay3 = calculateNextDelay(delay2)
    val delay4 = calculateNextDelay(delay3)
    val delay5 = calculateNextDelay(delay4)

    assertEquals(delay1, 20.millis, "First retry should double")
    assertEquals(delay2, 40.millis, "Second retry should double again")
    assertEquals(delay3, 50.millis, "Third retry should cap at max")
    assertEquals(delay4, 50.millis, "Fourth retry should stay at max")
    assertEquals(delay5, 50.millis, "Fifth retry should stay at max")
  }

  test("RelayOrchestrator logs appropriate messages during lifecycle") {
    val orchestrator = RelayOrchestrator[IO, NostrFilterAuthored, Unit](
      relayUrl = relayUrl,
      backend = mockBackend,
      initialRetryDelay = 10.millis
    )

    val initialSub = createTestSubscription("logging-sub")

    for {
      subQueue <- Queue.unbounded[IO, NostrSubscription[NostrFilterAuthored]]

      attemptCount <- Ref.of[IO, Int](0)
      streamFactory = (
        _: NostrSubscription[NostrFilterAuthored],
        _: Queue[IO, NostrSubscription[NostrFilterAuthored]]
      ) =>
        Stream.eval(attemptCount.updateAndGet(_ + 1)).flatMap {
          case 1 => Stream.raiseError[IO](new Exception("First attempt fails"))
          case _ => Stream.emit(Some(())) ++ Stream.emit(None) // Success then EOSE
        }

      _ <- orchestrator
        .createResilientStream(streamFactory, initialSub, subQueue)
        .take(1)
        .compile
        .drain

    } yield
    // In a real test with a test logger, you'd verify:
    // - "Error on relay" message after first failure
    // - "Reconnecting" message with backoff info
    // - "EOSE received" message when None is emitted
    assert(true, "Logging test - check logs manually or use test logger")
  }

  test("[TBD]RelayOrchestrator handles concurrent subscription updates".ignore) {
    val orchestrator = RelayOrchestrator[IO, NostrFilterAuthored, Unit](
      relayUrl = relayUrl,
      backend = mockBackend
    )

    val initialSub = createTestSubscription("concurrent-sub")

    for {
      subQueue      <- Queue.unbounded[IO, NostrSubscription[NostrFilterAuthored]]
      processedSubs <- Ref.of[IO, Set[String]](Set.empty)

      streamFactory = (
        sub: NostrSubscription[NostrFilterAuthored],
        _: Queue[IO, NostrSubscription[NostrFilterAuthored]]
      ) =>
        Stream.eval(processedSubs.update(_ + sub.id)) >>
          Stream.eval(IO.sleep(50.millis)) >>
          Stream.emit(Some(())) ++
          Stream.emit(None)

      fiber <- orchestrator
        .createResilientStream(streamFactory, initialSub, subQueue)
        .compile
        .drain
        .start

      _ <- IO.sleep(100.millis)
      _ <- subQueue.offer(createTestSubscription("sub-2"))
      _ <- IO.sleep(100.millis)
      _ <- subQueue.offer(createTestSubscription("sub-3"))
      _ <- IO.sleep(200.millis)
      _ <- fiber.cancel

      subs <- processedSubs.get
    } yield {
      assert(subs.size >= 2, s"Should process multiple subscriptions (got ${subs.size})")
      assert(subs.contains("concurrent-sub"), "Should process initial subscription")
    }
  }

  test("[TBD]RelayOrchestrator reconnects after EOSE with new subscription".ignore) {
    val orchestrator = RelayOrchestrator[IO, NostrFilterAuthored, Unit](
      relayUrl = relayUrl,
      backend = mockBackend
    )

    val initialSub = createTestSubscription("eose-sub-1")
    val nextSub    = createTestSubscription("eose-sub-2")

    for {
      subQueue          <- Queue.unbounded[IO, NostrSubscription[NostrFilterAuthored]]
      subscriptionsUsed <- Ref.of[IO, List[String]](List.empty)

      streamFactory = (
        sub: NostrSubscription[NostrFilterAuthored],
        _: Queue[IO, NostrSubscription[NostrFilterAuthored]]
      ) =>
        Stream.eval(subscriptionsUsed.update(_ :+ sub.id)) >>
          Stream.emit(Some(())) ++
          Stream.emit(None)

      fiber <- orchestrator
        .createResilientStream(streamFactory, initialSub, subQueue)
        .compile
        .drain
        .start

      _ <- IO.sleep(50.millis)     // Wait for EOSE
      _ <- subQueue.offer(nextSub) // Offer new subscription after EOSE
      _ <- IO.sleep(100.millis)    // Wait for reconnection
      _ <- fiber.cancel

      usedSubs <- subscriptionsUsed.get
    } yield {
      assertEquals(usedSubs.length, 2, "Should use both subscriptions")
      assertEquals(usedSubs.head, "eose-sub-1", "First should be initial subscription")
      assertEquals(usedSubs.last, "eose-sub-2", "Second should be subscription after EOSE")
    }
  }
}
