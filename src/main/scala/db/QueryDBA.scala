package com.zoomin.earth.datalake.db

import cats.effect.kernel.Concurrent
import cats.effect.{IO, Temporal}
import com.google.cloud.bigquery.*
import com.zoomin.earth.datalake.config.BigQueryConfig
import com.zoomin.earth.datalake.db.BigQueryClient.eventSchema
import com.zoomin.earth.datalake.models.BigQueryNostrAuthoredEvent
import org.typelevel.log4cats.Logger

import java.time.Instant
import scala.jdk.CollectionConverters

case class InsertResult(insertedCount: Int, errors: List[String])

object InsertResult {
  val EmptyNoErrors                      = InsertResult(0, List.empty)
  def Errored(errors: List[String])      = InsertResult(0, errors)
  def NoErrorsNotEmpty(resultCount: Int) = InsertResult(resultCount, List.empty)
}

trait DBClient[T, F[_]] {
  def getOrCreateTable(): F[Unit]
  def insertToBigQuery(events: List[T]): F[InsertResult]
}

object BigQueryClient {

  val eventSchema = Schema.of(
    Field.newBuilder("id", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
    Field.newBuilder("pubkey", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
    Field.newBuilder("created_at", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
    Field.newBuilder("kind", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
    Field.newBuilder("content", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("sig", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
    Field.newBuilder("tags", StandardSQLTypeName.STRING).setMode(Field.Mode.REPEATED).build(),
    Field.newBuilder("relay_url", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("processed_at", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
    Field.newBuilder("event_raw", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
  )

}

class BigQueryClient(private val bigQuery: BigQuery, config: BigQueryConfig)
  extends DBClient[BigQueryNostrAuthoredEvent, IO] {

  import com.zoomin.earth.datalake.datapipelines.DataPipelinesContext.logger

  def getOrCreateTable(): IO[Unit] = IO {
    val datasetInfo = DatasetInfo.newBuilder(config.datasetId).build()

    try
      bigQuery.create(datasetInfo)
    catch {
      case e: BigQueryException =>
        logger.debug(s"Dataset ${config.datasetId} already exists or creation failed: ${e.getMessage}")
    }

    val tableId                = TableId.of(config.datasetId, config.tableId)
    val tablePubKeysDistinctId = TableId.of(config.datasetId, config.monitoredPubKeyTableId)

    val schemapubKeys = Schema.of(
      Field.newBuilder("pubkey", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
      Field.newBuilder("ingested_at", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build()
    )

    val tableDefinition = StandardTableDefinition.of(eventSchema)
    val tableInfo       = TableInfo.newBuilder(tableId, tableDefinition).build()

    val tableDefinitionPubKeys = StandardTableDefinition.of(schemapubKeys)
    val tablePubKeyInfo        = TableInfo.newBuilder(tablePubKeysDistinctId, tableDefinitionPubKeys).build()

    try {
      bigQuery.create(tableInfo)
      bigQuery.create(tablePubKeyInfo)
      logger.info(s"Created table: $config.datasetId.$tableId")
    } catch {
      case _: BigQueryException =>
        logger.info(s"Table $config.datasetId.$tableId already exists")
    }
  }

  def insertToBigQuery(events: List[BigQueryNostrAuthoredEvent]): IO[InsertResult] =
    if events.isEmpty then {
      logger.debug("No events to insert, skipping BigQuery insertion")
      IO.pure(InsertResult.EmptyNoErrors)
    } else
      IO {
        val tableId = TableId.of(config.datasetId, config.tableId)
        import scala.jdk.CollectionConverters.*

        val rows = events.map { event =>
          val rowData = Map(
            "id"           -> event.id,
            "pubkey"       -> event.pubkey,
            "created_at"   -> event.created_at,
            "kind"         -> event.kind,
            "content"      -> event.content,
            "sig"          -> event.sig,
            "tags"         -> serializeTags(event.tags),
            "relay_url"    -> event.relay_url,
            "processed_at" -> Instant.now().getEpochSecond(),
            "event_raw"    -> event.toString()
          ).asJava

          InsertAllRequest.RowToInsert.of(rowData)
        }.asJava

        val insertRequest = InsertAllRequest
          .newBuilder(tableId)
          .setRows(rows)
          .build()

        val response = bigQuery.insertAll(insertRequest)
        if response.hasErrors() then {
          val errors = response.getInsertErrors().asScala.map(_.toString).toList
          InsertResult.Errored(errors)
        } else {
          logger.info(
            s"Successfully inserted ${events.size} events with the following ids: ${events.map(ev => s"'${ev.id}'").mkString(",")}"
          )
          InsertResult.NoErrorsNotEmpty(events.size)
        }
      }.handleErrorWith { err =>
        logger.error(s"Failed to insert to BigQuery: $err")
        IO.pure(InsertResult.Errored(List(err.getMessage)))
      }

  private def serializeTags(tags: List[List[String]]): java.util.List[String] = {
    import java.util.List as JList
    val buffer = new java.util.ArrayList[String]()
    tags.foreach(tag => buffer.add(tag.mkString(",")))
    buffer
  }

}
