package com.zoomin.earth.datalake.config

import com.zoomin.earth.datalake.config.{BigQueryConfig, NostrRelayConfig, StreamConfig}

import scala.concurrent.duration.FiniteDuration

case class PipelineConfig(
  relays: NostrRelayConfig,
  bigQuery: BigQueryConfig,
  pipeline: StreamConfig
)

case class NostrRelayConfig(
  urls: List[String],
  syncSince: Long,
  syncUntil: Long,
  publicKeysFollowFile: String
)

case class BigQueryConfig(
  projectId: String,
  datasetId: String,
  tableId: String,
  monitoredPubKeyTableId: String,
  credentialsPath: Option[String],
  batchSize: Int,
  batchTimeout: FiniteDuration,
  writeDisposition: String
)

case class StreamConfig(
  parallelism: Int,
  bufferSize: Int,
  reconnectDelay: FiniteDuration,
  maxReconnectAttempts: Int
)
