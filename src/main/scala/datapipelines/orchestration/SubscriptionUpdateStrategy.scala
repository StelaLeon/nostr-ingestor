package com.zoomin.earth.datalake.datapipelines.orchestration

import com.zoomin.earth.datalake.annotations.doc
import com.zoomin.earth.datalake.models.*

@doc("Encapsulates the business logic for when and how to update subscriptions")
trait SubscriptionUpdateStrategy[T <: NostrFilterBase] {

  @doc("""
   Determines if we should update the subscription based on accumulated events
   @return Some(newSubscription) if update needed, None otherwise
  """)
  def shouldUpdate(
    accumulatedEvents: List[NostrDataEvent],
    currentSubscription: NostrSubscription[T]
  ): Option[NostrSubscription[T]]

}

@doc("Updates subscription after a threshold of events, creating a sliding time window")
class TimeWindowUpdateStrategy[T <: NostrFilterBase](
  eventThreshold: Int = 100,
  timeWindowSeconds: Long = 7200,
  originalStartTime: Long
) extends SubscriptionUpdateStrategy[T] {

  def shouldUpdate(
    accumulatedEvents: List[NostrDataEvent],
    currentSubscription: NostrSubscription[T]
  ): Option[NostrSubscription[T]] =
    if accumulatedEvents.size >= 0 then {
      val oldestEventTime = accumulatedEvents.minBy(_.created_at).created_at

      if oldestEventTime <= originalStartTime then {
        None
      } else {
        val until = oldestEventTime - 1
        val since = (until - timeWindowSeconds).max(originalStartTime)

        val updatedFilters = currentSubscription.filters.map { filter =>
          updateFilter(filter, since, until)
        }

        Some(currentSubscription.copy(filters = updatedFilters))
      }
    } else {
      None
    }

  private def updateFilter(filter: T, since: Long, until: Long): T = filter.withBounds(since, until).asInstanceOf[T]

}
