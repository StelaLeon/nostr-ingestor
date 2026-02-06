package com.zoomin.earth.datalake.datapipelines.processing

import cats.effect.*
import cats.effect.kernel.Temporal
import scala.collection.immutable.Queue
import com.zoomin.earth.datalake.datapipelines.processing.Cache
import cats.syntax.all.*

trait Cache[F[_], K, V] {
  def get(key: K): F[Option[V]]
  def put(key: K, value: V): F[Unit]
  def remove(key: K): F[Unit]
  def clear: F[Unit]
}

object EventCache {

  /**
   * Creates an internally managed Cache. This is minimal and used for de-duplication of events
   *
   * @return A Ref to the Cache: threads safe, lock free, purely functional, kube-cache!
   */

  private case class CacheState[K, V](data: Map[K, V], order: Queue[K])
  private val maxSize = 200000 // for nostr id -> 50 MB

  def make[F[_]: Concurrent, K, V]: F[Cache[F, K, V]] =
    Ref.of[F, CacheState[K, V]](CacheState(Map.empty, Queue.empty)).map { ref =>
      new Cache[F, K, V] {

        def get(key: K): F[Option[V]] = ref.get.map(_.data.get(key))

        override def put(key: K, value: V): F[Unit] =
          ref.update { state =>
            if state.data.contains(key) then {
              state.copy(data = state.data + (key -> value))
            } else {
              val newData  = state.data + (key -> value)
              val newOrder = state.order.enqueue(key)

              if newOrder.size > maxSize then {
                val (oldest, remainingOrder) = newOrder.dequeue
                CacheState(newData - oldest, remainingOrder)
              } else {
                CacheState(newData, newOrder)
              }
            }
          }

        override def remove(key: K): F[Unit] =
          ref.update(s => s.copy(data = s.data - key, order = s.order.filterNot(_ == key)))

        override def clear: F[Unit] = ref.set(CacheState(Map.empty, Queue.empty))
      }
    }

}
