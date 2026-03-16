package com.zoomin.earth.datalake.db

import com.google.cloud.bigquery.{Field, Schema, StandardSQLTypeName}

import scala.compiletime.*
import scala.deriving.Mirror
import scala.jdk.CollectionConverters.*

trait BqType[A]:
  def sqlType: StandardSQLTypeName
  def mode: Field.Mode = Field.Mode.REQUIRED

object BqType:

  given BqType[String] = new BqType[String] {
    def sqlType = StandardSQLTypeName.STRING
  }

  given BqType[Long] = new BqType[Long] {
    def sqlType = StandardSQLTypeName.INT64
  }

  given BqType[Int] = new BqType[Int] {
    def sqlType = StandardSQLTypeName.INT64
  }

  given BqType[Double] = new BqType[Double] {
    def sqlType = StandardSQLTypeName.FLOAT64
  }

  given BqType[Boolean] = new BqType[Boolean] {
    def sqlType = StandardSQLTypeName.BOOL
  }

  given [A](using inner: BqType[A]): BqType[Option[A]] = new BqType[Option[A]] {
    override def sqlType: StandardSQLTypeName = inner.sqlType
    override def mode: Field.Mode             = Field.Mode.NULLABLE
  }

  given [A](using inner: BqType[A]): BqType[List[A]] = new BqType[List[A]] {
    override def sqlType: StandardSQLTypeName = inner.sqlType
    override def mode: Field.Mode             = Field.Mode.REPEATED
  }

  given BqType[List[List[String]]] = new BqType[List[List[String]]] {
    override def sqlType: StandardSQLTypeName = StandardSQLTypeName.STRING
    override def mode: Field.Mode             = Field.Mode.REPEATED
  }

trait BqValue[A]:
  def serialize(a: A): Any

object BqValue:
  given BqValue[String]  = a => a
  given BqValue[Long]    = a => a
  given BqValue[Int]     = a => a
  given BqValue[Double]  = a => a
  given BqValue[Boolean] = a => a

  given [A](using inner: BqValue[A]): BqValue[Option[A]] =
    a => a.map(inner.serialize).orNull

  given [A](using inner: BqValue[A]): BqValue[List[A]] =
    a => a.map(inner.serialize).asJava

  given BqValue[List[List[String]]] =
    a => a.map(_.mkString(",")).asJava

trait BqRow[A]:
  def toRow(a: A): Map[String, Any]

object BqRow:

  inline def derived[A](using m: Mirror.ProductOf[A]): BqRow[A] =
    new BqRow[A]:
      def toRow(a: A): Map[String, Any] =
        val labels = constValueTuple[m.MirroredElemLabels].toArray.map(_.asInstanceOf[String])
        val values = summonBqValues[m.MirroredElemTypes]
        val fields = a.asInstanceOf[Product].productIterator.toArray
        labels
          .zip(fields)
          .zip(values)
          .map { case ((label, field), bqValue) =>
            label -> bqValue.asInstanceOf[BqValue[Any]].serialize(field)
          }
          .toMap

  private inline def summonBqValues[T <: Tuple]: List[BqValue[?]] =
    inline erasedValue[T] match {
      case _: EmptyTuple => Nil
      case _: (h *: t)   => summonInline[BqValue[h]] :: summonBqValues[t]
    }

  def apply[A](using r: BqRow[A]): BqRow[A] = r

trait BqSchema[A]:
  def schema: Schema

object BqSchema {

  inline def derived[A](using m: Mirror.ProductOf[A]): BqSchema[A] =
    new BqSchema[A] {
      override def schema: Schema = {
        val labels = constValueTuple[m.MirroredElemLabels].toArray.map(_.asInstanceOf[String])
        val types  = summonBqTypes[m.MirroredElemTypes]

        val fields = labels.zip(types).map { (name, byType) =>
          Field
            .newBuilder(name, byType.sqlType)
            .setMode(byType.mode)
            .build()
        }

        Schema.of(fields.toList.asJava)
      }
    }

  private inline def summonBqTypes[T <: Tuple]: List[BqType[?]] =
    inline erasedValue[T] match {
      case _: EmptyTuple => Nil
      case _: (h *: t)   => scala.compiletime.summonInline[BqType[h]] :: summonBqTypes[t]
    }

  def apply[A](using s: BqSchema[A]): BqSchema[A] = s

  extension (derived: Schema)

    def validateAgainst(actual: Schema): Either[List[String], Unit] =
      val derivedFields = derived.getFields.iterator.asScala.map(f => f.getName -> f).toMap
      val actualFields  = actual.getFields.iterator.asScala.map(f => f.getName -> f).toMap

      val missingInBq = derivedFields.keySet
        .diff(actualFields.keySet)
        .map(name => s"  '$name' exists in code but not in BigQuery")
        .toList

      val missingInCode = actualFields.keySet
        .diff(derivedFields.keySet)
        .map(name => s"  '$name' exists in BigQuery but not in code")
        .toList

      val typeMismatches = derivedFields.keySet
        .intersect(actualFields.keySet)
        .flatMap { name =>
          val d = derivedFields(name)
          val a = actualFields(name)
          if d.getType == a.getType && d.getMode == a.getMode then None
          else Some(s"  '$name': code=${d.getType}/${d.getMode} bq=${a.getType}/${a.getMode}")
        }
        .toList

      val allMismatches = missingInBq ++ missingInCode ++ typeMismatches
      if allMismatches.isEmpty then Right(())
      else Left(allMismatches)

}
