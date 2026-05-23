package com.zoomin.earth.datalake.nostr

import cats.effect.IO
import com.zoomin.earth.datalake.models.{
  BigQueryNostrAuthoredEvent,
  EOSE,
  NostrDataEvent,
  NostrEvent,
  NostrFilterBase,
  NostrSubscription
}
import io.circe.parser.parse
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant

trait NostrOps(using logger: Logger[IO]) {
  import NostrSerDesContext.given
  import io.circe.*
  import io.circe.syntax.*

  extension [T <: NostrFilterBase](subscription: NostrSubscription[T])

    def toJson(using decoder: Encoder[T]) =
      Json
        .arr(
          Json.fromString("REQ"),
          Json.fromString(subscription.id),
          subscription.filters.head.asJson
        )
        .noSpaces

  extension (message: NostrDataEvent)

    def toNostrAuthored(relayUrl: String): BigQueryNostrAuthoredEvent = {
      logger.debug(s"Extension method: message:${message}")
      BigQueryNostrAuthoredEvent(
        id = message.id,
        pubkey = message.pubkey,
        created_at = message.created_at,
        kind = message.kind,
        tags = message.tags,
        content = message.content,
        sig = message.sig,
        relay_url = relayUrl,
        processed_at = Instant.now().getEpochSecond
      )
    }

}

object NostrOps extends NostrOps(using Slf4jLogger.getLogger[IO]) {}
