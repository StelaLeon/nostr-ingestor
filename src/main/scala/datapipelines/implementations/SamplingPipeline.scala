package com.zoomin.earth.datalake.datapipelines.implementations

import cats.effect.IOApp
import com.zoomin.earth.datalake.config.ConfigLoader
import com.zoomin.earth.datalake.datapipelines.DataPipelinesContext
import com.zoomin.earth.datalake.models.*
import com.zoomin.earth.datalake.nostr.NostrSerDesContext
import com.zoomin.earth.datalake.datapipelines.{DataPipeline, DataPipelinesContext}
import org.typelevel.log4cats.Logger

import java.time.Instant
import java.util.UUID

object SamplingPipeline extends IOApp {

  import NostrSerDesContext.given
  import cats.effect.*

  import org.typelevel.log4cats.slf4j.Slf4jLogger

  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  def run(args: List[String]): IO[ExitCode] =
    for {
      config <- ConfigLoader.load[IO]
      _      <- logger.info("Starting sync from " + config.relays.syncSince)
      pipeRes        = DataPipelinesContext.generalNostrPipeline(config)
      relays         = config.relays.urls
      subscriptionId = UUID.randomUUID().toString

      subscription = NostrSubscription[NostrFilter](
        id = subscriptionId,
        filters = List(
          NostrFilter(
            kinds = Some(Kind.ids),
            since = Some(config.relays.syncSince),
            until = Some(config.relays.syncUntil)
          )
        )
      )
      exitCode <- pipeRes
        .flatMap(_.createPipeline(relays, subscription))
        .map(_.as(ExitCode.Success))
        .use(_.compile.drain.as(ExitCode.Success))
        .handleErrorWith(err => logger.error(s"Pipeline error: $err") *> IO.pure(ExitCode.Error))
    } yield exitCode

}
