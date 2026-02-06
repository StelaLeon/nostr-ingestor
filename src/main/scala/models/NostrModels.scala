package com.zoomin.earth.datalake.models

import cats.effect.std.UUIDGen

import java.util.UUID

trait NostrEvent

case class NostrDataEvent(
  id: String,
  pubkey: String,
  created_at: Long,
  kind: Int,
  tags: List[List[String]],
  content: String,
  sig: String
) extends NostrEvent

case class EOSE(id: String) extends NostrEvent

case class NostrMessage(
  messageType: String,
  subscriptionId: String,
  event: Option[NostrDataEvent]
)

case class NostrPipeline[T <: NostrFilter](relays: List[String], subscription: NostrSubscription[T])

case class NostrSubscription[T <: NostrFilter](
  id: String = s"main_${System.currentTimeMillis()}",
  filters: List[T]
)

object EOSESubscription extends NostrSubscription[NostrFilter](filters = List.empty)

sealed trait NostrFilter {
  val since: Option[Long]
  val until: Option[Long]

  def withBounds(since: Long, until: Long): NostrFilter
}

case class NostrFilterUnauthored(
  kinds: Option[List[Int]] = None,
  authors: Option[List[String]] = None,
  override val since: Option[Long] = None,
  override val until: Option[Long] = None,
  limit: Option[Int] = None
) extends NostrFilter {
  def withBounds(s: Long, u: Long): NostrFilterUnauthored = this.copy(since = Some(s), until = Some(u))
}

case class NostrFilterAuthored(
  kinds: Option[List[Int]] = None,
  authors: Option[List[String]] = None,
  override val since: Option[Long] = None,
  override val until: Option[Long] = None
) extends NostrFilter {
  def withBounds(s: Long, u: Long): NostrFilterAuthored = this.copy(since = Some(s), until = Some(u))
}

case object NoFilter extends NostrFilter {
  val since: Option[Long]                       = None
  val until: Option[Long]                       = None
  def withBounds(s: Long, u: Long): NostrFilter = this
}
