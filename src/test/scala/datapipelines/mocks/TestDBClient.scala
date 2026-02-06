package com.zoomin.earth.datalake.datapipelines.mocks

import cats.effect.kernel.Concurrent
import com.zoomin.earth.datalake.db.{DBClient, InsertResult}
import cats.effect.kernel.{Concurrent, Temporal}
import cats.syntax.all.*
import cats.syntax.applicative.*
import cats.syntax.flatMap.*
import cats.syntax.functor.*
import com.zoomin.earth.datalake.db.DBClient
import com.zoomin.earth.datalake.models.{
  EOSE,
  NostrDataEvent,
  NostrEvent,
  NostrFilter,
  NostrFilterAuthored,
  NostrFilterUnauthored,
  NostrSubscription
}
import fs2.Stream
import io.circe.Encoder
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.typelevel.log4cats.Logger
import sttp.capabilities.WebSockets
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.testing.SttpBackendStub
import sttp.client3.{Response, SttpBackend}

class TestDBClient[T, F[_]: Concurrent] extends DBClient[T, F] {
  import cats.effect.kernel.Ref

  private var itemsRef: Ref[F, List[T]]        = _
  private var tableCreatedRef: Ref[F, Boolean] = _

  private val initRefs: F[Unit] = for {
    items <- Ref.of[F, List[T]](List.empty)
    table <- Ref.of[F, Boolean](false)
    _ = itemsRef = items
    _ = tableCreatedRef = table
  } yield ()

  def init(): F[Unit] = initRefs

  def getOrCreateTable(): F[Unit] =
    ensureInitialized() >> tableCreatedRef.set(true)

  def insertToBigQuery(events: List[T]): F[InsertResult] =
    ensureInitialized() >> itemsRef.update(_ ++ events) >> InsertResult.NoErrorsNotEmpty(events.size).pure[F]

  def getSavedItems: F[List[T]] =
    ensureInitialized() >> itemsRef.get

  def clear(): F[Unit] =
    ensureInitialized() >> itemsRef.set(List.empty)

  def count: F[Int] =
    ensureInitialized() >> itemsRef.get.map(_.size)

  def isTableCreated: F[Boolean] =
    ensureInitialized() >> tableCreatedRef.get

  private def ensureInitialized(): F[Unit] =
    if itemsRef == null then initRefs else Concurrent[F].unit
}

object TestDBClient {

  def apply[T, F[_]: Concurrent]: F[TestDBClient[T, F]] =
    for {
      client <- Concurrent[F].pure(new TestDBClient[T, F])
      _      <- client.init()
    } yield client

}
