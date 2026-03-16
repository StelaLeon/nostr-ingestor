package com.zoomin.earth.datalake.db

import cats.effect.{IO, Resource}
import com.google.cloud.bigquery.*
import com.zoomin.earth.datalake.config.BigQueryConfig
import com.zoomin.earth.datalake.models.{
  BigQueryNostrAuthoredEvent,
  BigQueryNostrAuthoredEventDec,
  BigQueryStalkingPubKey
}

import java.time.Instant
import scala.jdk.CollectionConverters.*

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

object BigQueryClient extends BigQueryTable[BigQueryNostrAuthoredEvent] {

  override protected def tableId(config: BigQueryConfig): TableId =
    TableId.of(config.datasetId, config.tableId)

  def make(bigQuery: BigQuery, config: BigQueryConfig): Resource[IO, BigQueryClient] =
    for _ <- Resource.eval(IO.println("before validating the schema") *> validateSchema(bigQuery, config))
    yield new BigQueryClient(bigQuery, config)

}

class BigQueryClient(private val bigQuery: BigQuery, config: BigQueryConfig)
  extends DBClient[BigQueryNostrAuthoredEvent, IO] {

  val eventSchema: Schema = BqSchema[BigQueryNostrAuthoredEventDec].schema

  import com.zoomin.earth.datalake.datapipelines.DataPipelinesContext.logger

  def getOrCreateTable(): IO[Unit] = IO.blocking {
    val datasetInfo = DatasetInfo.newBuilder(config.datasetId).build()

    try
      bigQuery.create(datasetInfo)
    catch {
      case e: BigQueryException =>
        logger.debug(s"Dataset ${config.datasetId} already exists or creation failed: ${e.getMessage}")
    }

    val tableId = BigQueryClient.tableId(config)

    val tableDefinition = StandardTableDefinition.of(eventSchema)
    val tableInfo       = TableInfo.newBuilder(tableId, tableDefinition).build()

    try {
      bigQuery.create(tableInfo)
      logger.info(s"Created table: $config.datasetId.$tableId")
    } catch {
      case _: BigQueryException =>
        logger.info(s"Table $config.datasetId.$tableId already exists")
    }
  }

  def insertToBigQuery(events: List[BigQueryNostrAuthoredEvent]): IO[InsertResult] =
    if events.isEmpty then IO.pure(InsertResult.EmptyNoErrors)
    else
      IO.blocking {
        val tableId = TableId.of(config.datasetId, config.tableId)
        import scala.jdk.CollectionConverters.*

        val row  = BqRow[BigQueryNostrAuthoredEvent]
        val rows = events.map { event =>
          val derived = row.toRow(event) ++ Map(
            "processed_at" -> Instant.now().getEpochSecond,
            "event_raw"    -> event.toString
          )
          InsertAllRequest.RowToInsert.of(derived.asJava)
        }.asJava

        val insertRequest = InsertAllRequest
          .newBuilder(tableId)
          .setRows(rows)
          .build()

        bigQuery.insertAll(insertRequest) // actual blocking network call
      }.flatMap { response =>
        if response.hasErrors then
          val errors = response.getInsertErrors.asScala.map(_.toString).toList
          logger.error(s"Failed to insert: ${errors.mkString(",")}") >>
            IO.pure(InsertResult.Errored(errors))
        else
          logger.info(s"Inserted ${events.size} events") >>
            IO.pure(InsertResult.NoErrorsNotEmpty(events.size))
      }.handleErrorWith { err =>
        logger.error(s"Exception during insert: $err") >>
          IO.pure(InsertResult.Errored(List(err.getMessage)))
      }

  private def serializeTags(tags: List[List[String]]): java.util.List[String] = {
    val buffer = new java.util.ArrayList[String]()
    tags.foreach(tag => buffer.add(tag.mkString(",")))
    buffer
  }

}
