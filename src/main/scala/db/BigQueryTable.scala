package com.zoomin.earth.datalake.db

import cats.effect.IO
import com.google.cloud.bigquery.*
import com.zoomin.earth.datalake.config.BigQueryConfig
import com.zoomin.earth.datalake.db.BqSchema.validateAgainst

trait BigQueryTable[T: BqSchema] {

  protected def tableId(config: BigQueryConfig): TableId

  protected def validateSchema(bq: BigQuery, config: BigQueryConfig): IO[Unit] =
    IO.blocking {
      Option(bq.getTable(tableId(config)))
    }.flatMap {
      case None =>
        IO.raiseError(new RuntimeException(s"Table ${tableId(config)} does not exist"))

      case Some(table) =>
        val actual  = table.getDefinition[StandardTableDefinition].getSchema
        val derived = BqSchema[T].schema

        derived.validateAgainst(actual) match
          case Right(_)         => IO.unit
          case Left(mismatches) =>
            IO.raiseError(
              new RuntimeException(
                s"Schema mismatch for ${tableId(config)}:\n${mismatches.mkString("\n")}"
              )
            )
    }

}
