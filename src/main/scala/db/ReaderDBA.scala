package com.zoomin.earth.datalake.db

import cats.effect.IO
import cats.effect.kernel.Resource
import com.google.cloud.bigquery.{BigQuery, QueryJobConfiguration, TableId}
import com.zoomin.earth.datalake.config.BigQueryConfig
import com.zoomin.earth.datalake.models.BigQueryStalkingPubKey
import fs2.*

import scala.jdk.CollectionConverters.*

trait ReaderDBA[T, F[_]] {
  def readPubKeys(query: String): Stream[F, T]
}

class StalkingPubKeyReader(private val bigQuery: BigQuery, config: BigQueryConfig)
  extends ReaderDBA[BigQueryStalkingPubKey, IO] {

  private val query: String =
    s"""
      SELECT pubkey, from, until, relay_url
      FROM `${config.projectId}.${config.datasetId}.${config.monitoredPubKeyTableId}`
    """ // @todo: test this, is it the same dataset id?

  def readPubKeys(query: String): Stream[IO, BigQueryStalkingPubKey] =
    Stream
      .eval(IO.blocking {
        val jobConfig = QueryJobConfiguration
          .newBuilder(query)
          .setUseLegacySql(false)
          .build()

        bigQuery
          .query(jobConfig)
          .iterateAll()
          .asScala
          .toList
      })
      .flatMap { rows =>
        Stream.emits(rows.map { row =>
          BigQueryStalkingPubKey(
            pubKey = row.get("pubkey").getStringValue,
            from = Option(row.get("from")).filterNot(_.isNull).map(_.getLongValue),
            until = Option(row.get("until")).filterNot(_.isNull).map(_.getLongValue),
            relay_url = row.get("relay_url").getStringValue
          )
        })
      }
      .handleErrorWith(err =>
        Stream.eval(IO(println("Failed to read the stalked pubkey"))) >>
          Stream.raiseError[IO](err)
      )

}

object StalkingPubKeyReader extends BigQueryTable[BigQueryStalkingPubKey] {

  def make(bigQuery: BigQuery, config: BigQueryConfig): Resource[IO, StalkingPubKeyReader] =
    for _ <- Resource.eval(validateSchema(bigQuery, config))
    yield new StalkingPubKeyReader(bigQuery, config)

  override protected def tableId(config: BigQueryConfig): TableId =
    TableId.of(config.datasetId, config.monitoredPubKeyTableId)

}
