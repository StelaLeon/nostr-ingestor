package com.zoomin.earth.datalake.datapipelines

import com.zoomin.earth.datalake.backends.websocket.{WebSocketClient, WebSocketOps}
import cats.Parallel
import cats.effect.implicits.{monadCancelOps_, parallelForGenSpawn}
import cats.effect.kernel.{Concurrent, Fiber, Ref, Sync}
import cats.effect.std.Queue
import cats.effect.{Resource, Temporal}
import cats.syntax.all.*
import com.zoomin.earth.datalake.config.PipelineConfig
import com.zoomin.earth.datalake.datapipelines.orchestration.{
  RelayOrchestrator,
  SubscriptionUpdateStrategy,
  TimeWindowUpdateStrategy
}
import com.zoomin.earth.datalake.datapipelines.processing.{Cache, EventCache}
import com.zoomin.earth.datalake.db.DBClient
import com.zoomin.earth.datalake.models.*
import com.zoomin.earth.datalake.nostr.NostrOps.*
import com.zoomin.earth.datalake.parser.NostrEventParser
import com.zoomin.earth.datalake.datapipelines.processing.{
  EventAction,
  EventProcessor,
  Process,
  ProcessingState,
  Skip,
  UpdateSubscription
}
import fs2.Stream
import io.circe.*
import org.typelevel.log4cats.Logger
import sttp.ws.WebSocketFrame

import scala.concurrent.duration.*

trait DataPipeline[T <: NostrFilter, O, F[_]: Concurrent](
  sinkBackend: DBClient[O, F],
  backend: WebSocketClient[F],
  f: (NostrDataEvent, String) => O,
  config: PipelineConfig,
  updateStrategy: SubscriptionUpdateStrategy[T]
)(using Concurrent[F], Temporal[F], Logger[F], com.zoomin.earth.datalake.parser.Parser[T]) {

  import com.zoomin.earth.datalake.datapipelines.DataPipelinesContext.logger

  type StreamResult = Option[Unit]

  def createAuthoredPipelines(
    conns: List[NostrPipeline[T]]
  )(using encoder: Encoder[T], P: Parallel[F]): Resource[F, Stream[F, StreamResult]] =
    for {
      pipelines <- conns.traverse(c => createPipeline(c.relays, c.subscription))
      _         <- Resource.eval(Logger[F].info("Done running stalking pipelines"))
    } yield Stream.emits(pipelines).parJoin(10)

  def createPipeline(nostrRelays: List[String], subscription: NostrSubscription[T])(using
    encoder: Encoder[T]
  ): Resource[F, Stream[F, StreamResult]] =
    for {
      _     <- Resource.eval(Logger[F].info(s"Creating pipeline for ${nostrRelays.size} relays"))
      _     <- Resource.eval(sinkBackend.getOrCreateTable())
      queue <- Resource.eval(Queue.unbounded[F, O])
      subsQ <- Resource.eval(Queue.unbounded[F, NostrSubscription[T]])
      cache <- Resource.make(EventCache.make[F, String, Long])(cache => cache.clear)

    } yield {
      val nostrStreams: List[Stream[F, StreamResult]] =
        nostrRelays.map(relay => createSource(subsQ, queue, subscription, relay, cache))

      val bigQuerySink = Stream
        .fromQueueUnterminated(queue)
        .groupWithin(config.bigQuery.batchSize, config.bigQuery.batchTimeout)
        .parEvalMap(maxConcurrent = 4)(events => sinkBackend.insertToBigQuery(events.toList).unlessA(events.isEmpty))
        .handleErrorWith(err => Stream.eval(Logger[F].error(s"BigQuery error: $err")) >> Stream.empty)

      Stream
        .emits(nostrStreams)
        .parJoinUnbounded
        .concurrently(bigQuerySink)
    }

  private def createSource(
    subsQ: Queue[F, NostrSubscription[T]],
    queue: Queue[F, O],
    subscription: NostrSubscription[T],
    relayUrl: String,
    cache: Cache[F, String, Long]
  )(using encoder: Encoder[T]): Stream[F, StreamResult] = {

    val orchestrator = RelayOrchestrator[F, T, O](
      relayUrl = relayUrl,
      backend = backend
    )

    def createStreamForSubscription(
      sub: NostrSubscription[T],
      subQueue: Queue[F, NostrSubscription[T]]
    ): Stream[F, StreamResult] =
      createWebSocketStream(queue, sub, relayUrl, subQueue, cache)

    orchestrator.createResilientStream(
      streamFactory = createStreamForSubscription,
      initialSubscription = subscription,
      subscriptionQueue = subsQ
    )
  }

  private def createWebSocketStream(
    queue: Queue[F, O],
    initialSubscription: NostrSubscription[T],
    relayUrl: String,
    subQ: Queue[F, NostrSubscription[T]],
    cache: Cache[F, String, Long]
  )(using encoder: Encoder[T]): Stream[F, StreamResult] =
    Stream.eval(subQ.offer(initialSubscription)) >> Stream.eval {
      backend
        .withConnection(relayUrl) { ws =>
          val subscribeMessage = initialSubscription.toJson

          for {
            _       <- ws.send(WebSocketFrame.text(subscribeMessage))
            _       <- Logger[F].info(subscribeMessage)
            _       <- Logger[F].info(s"Connected to $relayUrl and subscribed")
            currSub <- Ref.of[F, NostrSubscription[T]](initialSubscription)

            result <- subscriptionReceiver(ws, currSub, subQ)
              .flatMap { fiber =>
                receiveMessages(initialSubscription, ws, queue, subQ, relayUrl, cache)
                  .guarantee(fiber.cancel)
              }
          } yield result
        }
        .flatMap {
          case Right(result) => Concurrent[F].pure(result)
          case Left(err)     => Logger[F].error(s"WebSocket error: $err").as(None)
        }
    }

  private def subscriptionReceiver(
    ws: WebSocketOps[F],
    currSub: Ref[F, NostrSubscription[T]],
    subQ: Queue[F, NostrSubscription[T]]
  )(using encoder: Encoder[T]): F[Fiber[F, Throwable, Unit]] =
    Concurrent[F].start {
      def sendLoop(): F[Unit] =
        subQ.take.flatMap { newSub =>
          ws.send(WebSocketFrame.text(newSub.toJson)) >>
            Logger[F].info(s"Sent updated subscription: ${newSub.toString()} to") >>
            currSub.set(newSub) >>
            sendLoop()
        }

      sendLoop()
    }

  private def receiveMessages(
    subscription: NostrSubscription[T],
    ws: WebSocketOps[F],
    queue: Queue[F, O],
    subQ: Queue[F, NostrSubscription[T]],
    relayUrl: String,
    cache: Cache[F, String, Long]
  ): F[StreamResult] = {

    def loop(state: ProcessingState): F[StreamResult] =
      ws.receive().flatMap {
        case WebSocketFrame.Text(message, _, _) =>
          parseNostrMessage(message).flatMap {
            case Some(event: NostrDataEvent) =>
              cache.get(event.id).flatMap {
                case Some(_) =>
                  Logger[F].info(s"Event ${event.id} already in cache, skipping") >>
                    loop(state)
                case None =>
                  val action: EventAction[F, T] = EventProcessor.processEvent[F, T](
                    event,
                    state.accumulator,
                    subscription,
                    updateStrategy,
                    onUpdate = (sub: NostrSubscription[T]) => subQ.offer(sub)
                  )
                  action match {
                    case Skip()          => loop(state)
                    case Process(newAcc) =>
                      cache.put(event.id, event.created_at) >>
                        queue.offer(f(event, relayUrl)) >>
                        loop(ProcessingState(newAcc))
                    case UpdateSubscription(newSub, handler) =>
                      Logger[F].info(s"Updating subscription: ${newSub.filters}") >>
                        cache.clear >>
                        handler(newSub) >>
                        cache.put(event.id, event.created_at) >>
                        queue.offer(f(event, relayUrl)) >>
                        loop(ProcessingState.empty)
                  }
              }
            case Some(_: EOSE) =>
              Logger[F].info("EOSE received") >>
                loop(state)
            case other =>
              Logger[F].info(s"Invalid message: $other") >>
                loop(state)
          }
        case WebSocketFrame.Close(_, _) =>
          Logger[F].warn("WebSocket closed") >>
            Concurrent[F].pure(None)
        case other =>
          Logger[F].warn(s"Unexpected frame: $other") >>
            loop(state)
      }

    loop(ProcessingState.empty)
  }

  private def parseNostrMessage(message: String): F[Option[NostrEvent]] =
    Concurrent[F].pure {
      import com.zoomin.earth.datalake.parser.instances.authoredParser
      NostrEventParser.parseWith(message)
    }

  def logAndSleep[F[_]: Temporal: Logger](msg: String, delay: FiniteDuration): Stream[F, Nothing] =
    Stream.eval(Logger[F].info(msg)) >>
      Stream.sleep[F](delay) >>
      Stream.empty

  private def recoverStream[F[_]: Temporal: Logger, A](
    delay: FiniteDuration,
    msg: Throwable => String
  ): PartialFunction[Throwable, Stream[F, A]] = { case err =>
    logAndSleep[F](msg(err), delay)
  }

}
