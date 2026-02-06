package com.zoomin.earth.datalake.datapipelines.implementations

import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.zoomin.earth.datalake.config.ConfigLoader
import com.zoomin.earth.datalake.datapipelines.profiling.DatapipelineProfiler
import com.zoomin.earth.datalake.datapipelines.profiling.DatapipelineProfiler.interArrivalTimingPipe
import com.zoomin.earth.datalake.datapipelines.{DataPipeline, DataPipelinesContext}
import com.zoomin.earth.datalake.models.*
import com.zoomin.earth.datalake.nostr.NostrSerDesContext
import fs2.Stream
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant
import java.util.UUID
import scala.io.Source
import scala.util.Using

object StalkingPipeline extends IOApp {

  import com.zoomin.earth.datalake.nostr.NostrSerDesContext.given

  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  def run(args: List[String]): IO[ExitCode] = {
    val subscriptionId = UUID.randomUUID().toString

    def getSubscription(authors: List[String], since: Long, until: Long) = NostrSubscription[NostrFilterAuthored](
      id = subscriptionId,
      filters = List(
        NostrFilterAuthored(
          kinds = Some(Kind.ids),
          authors = Some(authors),
          since = Some(since),
          until = Some(until)
        )
      )
    )

    ConfigLoader.load[IO].flatMap { config =>
      val pubKeysToStalk = Using.resource(Source.fromFile(config.relays.publicKeysFollowFile)) { source =>
        source.getLines().toList
      }

      DataPipelinesContext
        .authoredNostrPipeline(config)
        .use { pipeRes =>
          val pipelines =
            pubKeysToStalk
              .grouped(100)
              .toList
              .map { users =>
                NostrPipeline(
                  relays = config.relays.urls,
                  subscription = getSubscription(authors = users, config.relays.syncSince, config.relays.syncUntil)
                )
              }

          Stream
            .emits(pipelines)
            .covary[IO]
            .map { pipeline =>
              Stream
                .resource(
                  pipeRes.createPipeline(
                    pipeline.relays,
                    pipeline.subscription
                  )
                )
                .flatMap { drain =>
                  Stream.eval(
                    logger.info(s"Pipeline running: ${pipeline.subscription}")
                  ) >>
                    drain >>
                    Stream.eval(
                      logger.info(s"Pipeline stopped: ${pipeline.subscription}")
                    )
                }
            }
            .parJoin(10)
            .compile
            .drain
            .as(ExitCode.Success)
        }
        .handleErrorWith { err =>
          logger.info(s"Pipeline error: $err") *> IO.pure(ExitCode.Error)
        }
    }
  }

}
