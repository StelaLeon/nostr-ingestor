package com.zoomin.earth.datalake.datapipelines.mocks

import cats.syntax.all.*
import com.zoomin.earth.datalake.models.{NostrDataEvent, NostrEvent, NostrFilter}
import io.circe.*
import io.circe.generic.auto.*
import io.circe.parser.*
import io.circe.syntax.*

object NostrProtocol {

  def generateMockEvents(count: Int): List[NostrEvent] =
    (1 to count).map { i =>
      NostrDataEvent(
        id = s"event_id_$i",
        pubkey = s"pubkey_${i % 5}",
        created_at = System.currentTimeMillis() / 1000 - (count - i) * 3600,
        kind = if i % 3 == 0 then 1 else 0,
        tags = List.empty,
        content = s"This is mock event #$i with some test content",
        sig = s"signature_$i"
      )
    }.toList

  def parseSubscription(msg: String): Option[(String, List[NostrFilter])] =
    parse(msg).toOption.flatMap { json =>
      json.asArray.flatMap { arr =>
        if arr.headOption.exists(_.asString.contains("REQ")) then {
          for {
            subId <- arr.lift(1).flatMap(_.asString)
            filters = arr.drop(2).flatMap(_.as[NostrFilter].toOption).toList
          } yield (subId, filters)
        } else None
      }
    }

  def parseClose(msg: String): Option[String] =
    parse(msg).toOption.flatMap { json =>
      json.asArray.flatMap { arr =>
        if arr.headOption.exists(_.asString.contains("CLOSE")) then {
          arr.lift(1).flatMap(_.asString)
        } else None
      }
    }

  def encodeEvent(subId: String, event: NostrDataEvent): String =
    Json
      .arr(
        Json.fromString("EVENT"),
        Json.fromString(subId),
        event.asJson
      )
      .noSpaces

  def encodeEOSE(subId: String): String =
    Json
      .arr(
        Json.fromString("EOSE"),
        Json.fromString(subId)
      )
      .noSpaces

  def encodeOK(eventId: String, accepted: Boolean, message: String): String =
    Json
      .arr(
        Json.fromString("OK"),
        Json.fromString(eventId),
        Json.fromBoolean(accepted),
        Json.fromString(message)
      )
      .noSpaces

  def encodeNotice(message: String): String =
    Json
      .arr(
        Json.fromString("NOTICE"),
        Json.fromString(message)
      )
      .noSpaces

}
