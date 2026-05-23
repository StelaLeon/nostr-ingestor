package com.zoomin.earth.datalake.datapipelines

import com.google.cloud.bigquery.BigQueryOptions
import com.zoomin.earth.datalake.backends.websocket.WebSocketClient
import com.zoomin.earth.datalake.config.PipelineConfig
import com.zoomin.earth.datalake.datapipelines.orchestration.TimeWindowUpdateStrategy
import com.zoomin.earth.datalake.db.BigQueryClient
import com.zoomin.earth.datalake.models.{BigQueryNostrAuthoredEvent, NostrDataEvent, NostrFilter}
import org.asynchttpclient.DefaultAsyncHttpClientConfig
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import sttp.client3.asynchttpclient.fs2.AsyncHttpClientFs2Backend

object DataPipelinesContext {

  import cats.effect.*
  import com.zoomin.earth.datalake.nostr.NostrOps
  import NostrOps.*
  import com.zoomin.earth.datalake.parser.instances.given
  import sttp.client3.*

  given logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  val customClientConfig: DefaultAsyncHttpClientConfig = new DefaultAsyncHttpClientConfig.Builder()
    .setWebSocketMaxFrameSize(3 * 1024 * 1024) // 1 MB
    .build()

  def authoredNostrPipeline(
    config: PipelineConfig
  ): Resource[IO, DataPipeline[NostrFilter, BigQueryNostrAuthoredEvent, IO]] =
    for {

      backend  <- AsyncHttpClientFs2Backend.resourceUsingConfig[IO](customClientConfig)
      bigQuery <- Resource.eval(
        IO(BigQueryOptions.newBuilder().setProjectId(config.bigQuery.projectId).build().getService)
      )
      bqClient <- BigQueryClient.make(bigQuery, config.bigQuery)
      subscriptionUpdateStrategy = TimeWindowUpdateStrategy[NostrFilter](originalStartTime = config.relays.syncSince)
    } yield new DataPipeline[NostrFilter, BigQueryNostrAuthoredEvent, IO](
      bqClient,
      WebSocketClient(backend),
      (ev: NostrDataEvent, relayUrl: String) => ev.toNostrAuthored(relayUrl),
      config,
      subscriptionUpdateStrategy
    ) {}

  def generalNostrPipeline(
    config: PipelineConfig
  ): Resource[IO, DataPipeline[NostrFilter, BigQueryNostrAuthoredEvent, IO]] =
    for {

      backend  <- AsyncHttpClientFs2Backend.resourceUsingConfig[IO](customClientConfig)
      bigQuery <- Resource.eval(
        IO(BigQueryOptions.newBuilder().setProjectId(config.bigQuery.projectId).build().getService)
      )
      bqClient                   = new BigQueryClient(bigQuery, config.bigQuery)
      subscriptionUpdateStrategy = TimeWindowUpdateStrategy[NostrFilter](originalStartTime = config.relays.syncSince)

    } yield new DataPipeline[NostrFilter, BigQueryNostrAuthoredEvent, IO](
      bqClient,
      WebSocketClient(backend),
      (ev: NostrDataEvent, r: String) => ev.toNostrAuthored(r),
      config,
      subscriptionUpdateStrategy
    ) {}

}
