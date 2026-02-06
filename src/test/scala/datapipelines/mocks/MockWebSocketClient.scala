package com.zoomin.earth.datalake.datapipelines.mocks

import cats.effect.IO
import cats.effect.std.Queue
import sttp.ws.WebSocketFrame
import cats.effect.{IO, Ref}
import cats.effect.std.Queue
import cats.effect.kernel.Concurrent
import cats.syntax.all.*
import sttp.ws.WebSocketFrame
import io.circe.syntax.*
import io.circe.Json
import com.zoomin.earth.datalake.backends.websocket.{WebSocketClient, WebSocketOps}
import cats.effect.IO
import cats.effect.std.Queue
import sttp.ws.WebSocketFrame
import cats.effect.{IO, Ref}
import cats.effect.std.Queue
import cats.effect.kernel.Concurrent
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import sttp.ws.WebSocketFrame
import io.circe.syntax.*
import io.circe.{Json, parser}

/**
 * Mock WebSocketClient for testing that returns predefined Nostr events.
 */
object MockWebSocketClient {

  /**
   * Creates a mock WebSocketClient that simulates a Nostr relay with filtering.
   *
   * @param allEvents Map of relay URL to list of Nostr events
   * @return A WebSocketClient that filters and replays events based on REQ subscriptions
   */
  def withFilteredEvents(
    allEvents: Map[String, List[Json]]
  )(using Concurrent[IO]): IO[WebSocketClient[IO]] =
    for {
      sentMessages <- Ref.of[IO, List[WebSocketFrame]](List.empty)
    } yield new WebSocketClient[IO] {
      override def withConnection[A](relayUrl: String)(use: WebSocketOps[IO] => IO[A]): IO[Either[String, A]] =
        for {
          messageQueue <- Queue.unbounded[IO, WebSocketFrame]

          mockOps = new WebSocketOps[IO] {
            override def send(frame: WebSocketFrame): IO[Unit] =
              frame match {
                case WebSocketFrame.Text(text, _, _) =>
                  parser.parse(text).flatMap(_.as[List[Json]]) match {
                    case Right(reqArray) if reqArray.headOption.exists(_.asString.contains("REQ")) =>
                      val subscriptionId = reqArray.lift(1).flatMap(_.asString).getOrElse("unknown")
                      val filters        = reqArray.drop(2)

                      val eventsForRelay = allEvents.getOrElse(relayUrl, List.empty)
                      val filteredEvents = filterEvents(eventsForRelay, filters)

                      val queueEvents = filteredEvents.traverse { event =>
                        val eventMessage = Json
                          .arr(
                            Json.fromString("EVENT"),
                            Json.fromString(subscriptionId),
                            event
                          )
                          .noSpaces
                        messageQueue.offer(WebSocketFrame.text(eventMessage))
                      }

                      val eoseMessage = Json
                        .arr(
                          Json.fromString("EOSE"),
                          Json.fromString(subscriptionId)
                        )
                        .noSpaces

                      queueEvents *>
                        messageQueue.offer(WebSocketFrame.text(eoseMessage)) *>
                        messageQueue.offer(WebSocketFrame.Close(1000, "EOSE reached")) *>
                        sentMessages.update(_ :+ frame)

                    case _ =>
                      sentMessages.update(_ :+ frame)
                  }

                case _ =>
                  sentMessages.update(_ :+ frame)
              }

            override def receive(): IO[WebSocketFrame] =
              messageQueue.take
          }

          result <- use(mockOps).map(Right(_)).handleErrorWith(e => IO.pure(Left(e.getMessage)))
        } yield result
    }

  /**
   * Creates a mock WebSocketClient that simulates a Nostr relay.
   *
   * @param events List of Nostr events to return (as JSON objects)
   * @param subscriptionId The subscription ID to use in EVENT messages
   * @return A WebSocketClient that replays the provided events
   */
  def withEvents(
    events: List[Json],
    subscriptionId: String = "test-sub"
  )(using Concurrent[IO]): IO[WebSocketClient[IO]] =
    for {
      messageQueue <- Queue.unbounded[IO, WebSocketFrame]
      sentMessages <- Ref.of[IO, List[WebSocketFrame]](List.empty)

      _ <- events.traverse { event =>
        val eventMessage = Json
          .arr(
            Json.fromString("EVENT"),
            Json.fromString(subscriptionId),
            event
          )
          .noSpaces
        messageQueue.offer(WebSocketFrame.text(eventMessage))
      }

      eoseMessage = Json
        .arr(
          Json.fromString("EOSE"),
          Json.fromString(subscriptionId)
        )
        .noSpaces
      _ <- messageQueue.offer(WebSocketFrame.text(eoseMessage))

    } yield new WebSocketClient[IO] {
      override def withConnection[A](relayUrl: String)(use: WebSocketOps[IO] => IO[A]): IO[Either[String, A]] = {
        val mockOps = new WebSocketOps[IO] {
          override def send(frame: WebSocketFrame): IO[Unit] =
            sentMessages.update(_ :+ frame)

          override def receive(): IO[WebSocketFrame] =
            messageQueue.take
        }

        use(mockOps).map(Right(_)).handleErrorWith(e => IO.pure(Left(e.getMessage)))
      }
    }

  /**
   * Filters events based on Nostr filter criteria.
   * Supports 'since' and 'until' timestamp filtering.
   */
  private def filterEvents(events: List[Json], filters: List[Json]): List[Json] = {
    if filters.isEmpty then return events

    events.filter { event =>
      val eventTimestamp = event.hcursor.get[Long]("created_at").toOption

      filters.exists { filter =>
        val since   = filter.hcursor.get[Long]("since").toOption
        val until   = filter.hcursor.get[Long]("until").toOption
        val kinds   = filter.hcursor.get[List[Int]]("kinds").toOption
        val authors = filter.hcursor.get[List[String]]("authors").toOption

        val timestampMatch = eventTimestamp.forall { ts =>
          since.forall(_ <= ts) && until.forall(_ >= ts)
        }

        val kindMatch = kinds.forall { ks =>
          event.hcursor.get[Int]("kind").toOption.exists(ks.contains)
        }

        val authorMatch = authors.forall { as =>
          event.hcursor.get[String]("pubkey").toOption.exists(as.contains)
        }

        timestampMatch && kindMatch && authorMatch
      }
    }
  }

  /**
   * Creates a mock WebSocketClient with custom behavior.
   * Useful for testing error cases or specific interaction patterns.
   *
   * @param messagesToSend Queue of WebSocket frames to send
   * @param sentMessagesRef Ref to track messages sent by the client
   */
  def withQueue(
    messagesToSend: Queue[IO, WebSocketFrame],
    sentMessagesRef: Ref[IO, List[WebSocketFrame]]
  ): WebSocketClient[IO] = new WebSocketClient[IO] {

    override def withConnection[A](relayUrl: String)(use: WebSocketOps[IO] => IO[A]): IO[Either[String, A]] = {
      val mockOps = new WebSocketOps[IO] {
        override def send(frame: WebSocketFrame): IO[Unit] =
          sentMessagesRef.update(_ :+ frame)

        override def receive(): IO[WebSocketFrame] =
          messagesToSend.take
      }

      use(mockOps).map(Right(_)).handleErrorWith(e => IO.pure(Left(e.getMessage)))
    }

  }

  /**
   * Creates a failing mock WebSocketClient that always returns an error.
   * Useful for testing error handling.
   */
  def failing(errorMessage: String = "Connection failed"): WebSocketClient[IO] =
    new WebSocketClient[IO] {
      override def withConnection[A](relayUrl: String)(use: WebSocketOps[IO] => IO[A]): IO[Either[String, A]] =
        IO.pure(Left(errorMessage))
    }

}
