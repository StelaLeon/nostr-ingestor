package com.zoomin.earth.datalake.datapipelines.orchestration

import cats.effect.Temporal
import cats.effect.std.Queue
import cats.syntax.all.*
import com.zoomin.earth.datalake.backends.websocket.WebSocketClient
import com.zoomin.earth.datalake.models.{NostrFilterBase, NostrSubscription}
import fs2.Stream
import org.typelevel.log4cats.Logger
import com.zoomin.earth.datalake.annotations.doc


import scala.concurrent.duration.*

@doc(
  """
    |Orchestrates a single relay connection with reconnection, subscription management, and EOSE handling
    | *
    | * This class encapsulates ALL relay-specific concerns:
    | * - WebSocket connection lifecycle
    | * - Reconnection with exponential backoff
    | * - Subscription updates
    | * - EOSE (End of Stored Events) handling
    | *
    | * @param StreamResult Should be Option[Unit] - None signals EOSE (End of Stored Events)
    |""".stripMargin)
class RelayOrchestrator[F[_]: Temporal: Logger, T <: NostrFilterBase, O](
  relayUrl: String,
  backend: WebSocketClient[F],
  initialRetryDelay: FiniteDuration = 1.second,
  maxRetryDelay: FiniteDuration = 30.seconds
) {

  type StreamResult = Option[Unit]

  @doc(
    """
      Creates a resilient stream for this relay that automatically reconnects on failure

          @param streamFactory Function that creates the WebSocket stream for a given subscription
          @param initialSubscription The starting subscription
          @param subscriptionQueue Queue for receiving subscription updates""".stripMargin
  )
  def createResilientStream(
    streamFactory: (NostrSubscription[T], Queue[F, NostrSubscription[T]]) => Stream[F, StreamResult],
    initialSubscription: NostrSubscription[T],
    subscriptionQueue: Queue[F, NostrSubscription[T]],
    getNextSubscription: F[Option[NostrSubscription[T]]]
  ): Stream[F, StreamResult] =
    Stream.eval(subscriptionQueue.offer(initialSubscription)) >>
      reconnectLoop(initialSubscription, initialRetryDelay, streamFactory, subscriptionQueue, getNextSubscription)

  private def reconnectLoop(
    currentSubscription: NostrSubscription[T],
    retryDelay: FiniteDuration,
    streamFactory: (NostrSubscription[T], Queue[F, NostrSubscription[T]]) => Stream[F, StreamResult],
    subscriptionQueue: Queue[F, NostrSubscription[T]],
    getNextSubscription: F[Option[NostrSubscription[T]]]
  ): Stream[F, StreamResult] =
    streamFactory(currentSubscription, subscriptionQueue)
      .flatMap { (res: StreamResult) =>
        res match {
          case Some(value) =>
            Stream.emit(Some(value)) ++
              Stream.eval(getNextSubscription).flatMap {
                case Some(nextSub) =>
                  Stream.eval(Logger[F].info(s"EOSE on $relayUrl, advancing to next window")) >>
                    reconnectLoop(nextSub, initialRetryDelay, streamFactory, subscriptionQueue, getNextSubscription)
                case None =>
                  Stream
                    .eval(Logger[F].info(s"EOSE on $relayUrl, no more windows — done"))
                    .flatMap(_ => Stream.empty.covaryAll[F, StreamResult])
              }
          case None =>
            Stream
              .eval(Logger[F].info(s"WebSocket closed on $relayUrl"))
              .flatMap(_ => Stream.empty.covaryAll[F, StreamResult])
        }
      }
      .handleErrorWith(error =>
        handleConnectionError(
          error,
          currentSubscription,
          retryDelay,
          streamFactory,
          subscriptionQueue,
          getNextSubscription
        )
      )

  private def handleConnectionError(
    error: Throwable,
    currentSubscription: NostrSubscription[T],
    retryDelay: FiniteDuration,
    streamFactory: (NostrSubscription[T], Queue[F, NostrSubscription[T]]) => Stream[F, StreamResult],
    subscriptionQueue: Queue[F, NostrSubscription[T]],
    getNextSubscription: F[Option[NostrSubscription[T]]]
  ): Stream[F, StreamResult] =
    Stream.eval(Logger[F].error(s"Error on relay $relayUrl: $error")) >>
      Stream.sleep(retryDelay) >>
      Stream.eval(getNextSubscription).flatMap {
        case Some(newSubscription) =>
          val resetDelay = initialRetryDelay.min(maxRetryDelay)
          Stream.eval(Logger[F].info(s"Reconnecting $relayUrl with updated subscription")) >>
            reconnectLoop(newSubscription, resetDelay, streamFactory, subscriptionQueue, getNextSubscription)
        case None =>
          val nextDelay = (retryDelay * 2).min(maxRetryDelay)
          Stream.eval(Logger[F].info(s"Reconnecting $relayUrl with backoff ${nextDelay.toSeconds}s")) >>
            reconnectLoop(currentSubscription, nextDelay, streamFactory, subscriptionQueue, getNextSubscription)
      }

}

object RelayOrchestrator {

  def apply[F[_]: Temporal: Logger, T <: NostrFilterBase, O](
    relayUrl: String,
    backend: WebSocketClient[F],
    initialRetryDelay: FiniteDuration = 1.second,
    maxRetryDelay: FiniteDuration = 30.seconds
  ): RelayOrchestrator[F, T, O] =
    new RelayOrchestrator(relayUrl, backend, initialRetryDelay, maxRetryDelay)

}
