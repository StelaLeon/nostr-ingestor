package com.zoomin.earth.datalake.nostr

import io.circe.*
import io.circe.generic.semiauto.*
import com.zoomin.earth.datalake.models.*

object NostrSerDesContext {

  given Decoder[NostrDataEvent] = deriveDecoder[NostrDataEvent]

  given Encoder[NostrDataEvent] = deriveEncoder[NostrDataEvent]

  given Decoder[EOSE] = Decoder.instance { cursor =>
    cursor.value.asArray match {
      case Some(arr) if arr.size >= 2 =>
        arr(0).asString match {
          case Some("EOSE") =>
            arr(1).asString match {
              case Some(id) => Right(EOSE(id))
              case None     => Left(DecodingFailure("Second element not a string", cursor.history))
            }
          case _ => Left(DecodingFailure("First element not EOSE", cursor.history))
        }
      case _ => Left(DecodingFailure("Not an array with at least 2 elements", cursor.history))
    }
  }

  given Encoder[EOSE] = deriveEncoder[EOSE]

  given Decoder[NostrFilter] = deriveDecoder[NostrFilter]

  given Encoder[NostrFilter] = deriveEncoder[NostrFilter].mapJson(_.dropNullValues)

  given Decoder[NostrSubscription[NostrFilter]] = deriveDecoder[NostrSubscription[NostrFilter]]

  given Encoder[NostrSubscription[NostrFilter]] =
    deriveEncoder[NostrSubscription[NostrFilter]].mapJson(_.dropNullValues)

}
