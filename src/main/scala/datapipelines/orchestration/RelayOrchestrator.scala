package com.zoomin.earth.datalake.datapipelines.orchestration

import cats.effect.Temporal
import cats.effect.std.Queue
import cats.syntax.all.*
import com.zoomin.earth.datalake.backends.websocket.WebSocketClient
import com.zoomin.earth.datalake.config.ConfigLoader.{
  given_ConfigReader_BigQueryConfig,
  given_ConfigReader_PipelineConfig
}
import com.zoomin.earth.datalake.models.{NostrFilter, NostrSubscription}
import fs2.Stream
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.*

/**
 * Orchestrates a single relay connection with reconnection, subscription management, and EOSE handling
 *
 * This class encapsulates ALL relay-specific concerns:
 * - WebSocket connection lifecycle
 * - Reconnection with exponential backoff
 * - Subscription updates
 * - EOSE (End of Stored Events) handling
 *
 * @param StreamResult Should be Option[Unit] - None signals EOSE (End of Stored Events)
 */
class RelayOrchestrator[F[_]: Temporal: Logger, T <: NostrFilter, O](
  relayUrl: String,
  backend: WebSocketClient[F],
  initialRetryDelay: FiniteDuration = 1.second,
  maxRetryDelay: FiniteDuration = 30.seconds
) {

  // StreamResult is Option[Unit] where None means EOSE
  type StreamResult = Option[Unit]

  /**
   * Creates a resilient stream for this relay that automatically reconnects on failure
   *
   * @param streamFactory Function that creates the WebSocket stream for a given subscription
   * @param initialSubscription The starting subscription
   * @param subscriptionQueue Queue for receiving subscription updates
   */
  def createResilientStream(
    streamFactory: (NostrSubscription[T], Queue[F, NostrSubscription[T]]) => Stream[F, StreamResult],
    initialSubscription: NostrSubscription[T],
    subscriptionQueue: Queue[F, NostrSubscription[T]]
  ): Stream[F, StreamResult] =
    Stream.eval(subscriptionQueue.offer(initialSubscription)) >>
      reconnectLoop(initialSubscription, initialRetryDelay, streamFactory, subscriptionQueue)

  private def reconnectLoop(
    currentSubscription: NostrSubscription[T],
    retryDelay: FiniteDuration,
    streamFactory: (NostrSubscription[T], Queue[F, NostrSubscription[T]]) => Stream[F, StreamResult],
    subscriptionQueue: Queue[F, NostrSubscription[T]]
  ): Stream[F, StreamResult] =
    streamFactory(currentSubscription, subscriptionQueue)
      .flatMap { (res: StreamResult) =>
        res match {
          case Some(value) => Stream.emit(Some(value))
          case None =>
            Stream
              .eval(Logger[F].info(s"EOSE received for ${currentSubscription.id}"))
              .flatMap(_ => Stream.empty.covaryAll[F, StreamResult])
        }
      }
      .handleErrorWith(error =>
        handleConnectionError(error, currentSubscription, retryDelay, streamFactory, subscriptionQueue)
      )
      .onFinalize(
        logEOSEReceived(subscriptionQueue)
      )

  private def handleConnectionError(
    error: Throwable,
    currentSubscription: NostrSubscription[T],
    retryDelay: FiniteDuration,
    streamFactory: (NostrSubscription[T], Queue[F, NostrSubscription[T]]) => Stream[F, StreamResult],
    subscriptionQueue: Queue[F, NostrSubscription[T]]
  ): Stream[F, StreamResult] =
    Stream.eval(Logger[F].error(s"Error on relay $relayUrl: $error")) >>
      Stream.sleep(retryDelay) >>
      Stream.eval(subscriptionQueue.tryTake).flatMap {
        case Some(newSubscription) =>
          val resetDelay = initialRetryDelay.min(maxRetryDelay)
          Stream.eval(Logger[F].info(s"Reconnecting $relayUrl with updated subscription")) >>
            reconnectLoop(newSubscription, resetDelay, streamFactory, subscriptionQueue)

        case None =>
          val nextDelay = (retryDelay * 2).min(maxRetryDelay)
          Stream.eval(Logger[F].info(s"Reconnecting $relayUrl with backoff ${nextDelay.toSeconds}s")) >>
            reconnectLoop(currentSubscription, nextDelay, streamFactory, subscriptionQueue)
      }

  private def logEOSEReceived(subscriptionQueue: Queue[F, NostrSubscription[T]]): F[Unit] =
    subscriptionQueue.tryTake.flatMap {
      case Some(_) =>
        Logger[F].info(s"EOSE received on $relayUrl, new subscription queued")
      case None =>
        Logger[F].info(s"EOSE received on $relayUrl, waiting for new subscription")
    }

  private def reconnectAfterEOSE(
    streamFactory: (NostrSubscription[T], Queue[F, NostrSubscription[T]]) => Stream[F, StreamResult],
    subscriptionQueue: Queue[F, NostrSubscription[T]]
  ): Stream[F, StreamResult] =
    Stream.eval(subscriptionQueue.take).flatMap { newSubscription =>
      Stream.eval(Logger[F].info(s"Reconnecting $relayUrl after EOSE with new subscription")) >>
        reconnectLoop(newSubscription, initialRetryDelay, streamFactory, subscriptionQueue)
    }

}

object RelayOrchestrator {

  def apply[F[_]: Temporal: Logger, T <: NostrFilter, O](
    relayUrl: String,
    backend: WebSocketClient[F],
    initialRetryDelay: FiniteDuration = 1.second,
    maxRetryDelay: FiniteDuration = 30.seconds
  ): RelayOrchestrator[F, T, O] =
    new RelayOrchestrator(relayUrl, backend, initialRetryDelay, maxRetryDelay)

}
