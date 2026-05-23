package com.zoomin.earth.datalake.datapipelines.processing

import com.zoomin.earth.datalake.datapipelines.orchestration.SubscriptionUpdateStrategy
import com.zoomin.earth.datalake.models.{NostrDataEvent, NostrFilterBase, NostrSubscription}

case class ProcessingState(accumulator: List[NostrDataEvent])

object ProcessingState {
  def empty: ProcessingState = ProcessingState(List.empty)
}

sealed trait EventAction[F[_], +T <: NostrFilterBase]
case class Skip[F[_]]()                                extends EventAction[F, Nothing]
case class Process[F[_]](newAcc: List[NostrDataEvent]) extends EventAction[F, Nothing]

case class UpdateSubscription[F[_], T <: NostrFilterBase](
  newSub: NostrSubscription[T],
  handler: NostrSubscription[T] => F[Unit]
) extends EventAction[F, T]

object EventProcessor {

  def processEvent[F[_], T <: NostrFilterBase](
    event: NostrDataEvent,
    currentAcc: List[NostrDataEvent],
    subscription: NostrSubscription[T],
    updateStrategy: SubscriptionUpdateStrategy[T],
    onUpdate: NostrSubscription[T] => F[Unit]
  ): EventAction[F, T] = {
    val updatedAcc = currentAcc :+ event
    updateStrategy.shouldUpdate(updatedAcc, subscription) match {
      case Some(newSub) => UpdateSubscription(newSub, onUpdate)
      case None         => Process(updatedAcc)
    }
  }

}
