package com.zoomin.earth.datalake.models

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

case class NostrPipeline[T <: NostrFilterBase](relays: List[String], subscription: NostrSubscription[T])

case class NostrSubscription[T <: NostrFilterBase](
  id: String = s"main_${System.currentTimeMillis()}",
  filters: List[T]
)

object EOSESubscription extends NostrSubscription[NostrFilterBase](filters = List.empty)

sealed trait NostrFilterBase {
  val since: Option[Long]
  val until: Option[Long]

  def withBounds(since: Long, until: Long): NostrFilterBase
}

case class NostrFilter(
  kinds: Option[List[Int]] = None,
  authors: Option[List[String]] = None,
  override val since: Option[Long] = None,
  override val until: Option[Long] = None,
  limit: Option[Int] = None
) extends NostrFilterBase {
  def withBounds(s: Long, u: Long): NostrFilter = this.copy(since = Some(s), until = Some(u))
}

case object NoFilter extends NostrFilterBase {
  val since: Option[Long]                           = None
  val until: Option[Long]                           = None
  def withBounds(s: Long, u: Long): NostrFilterBase = this
}
