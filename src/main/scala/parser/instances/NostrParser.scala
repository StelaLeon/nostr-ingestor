package com.zoomin.earth.datalake.parser.instances

import cats.effect.IO
import com.zoomin.earth.datalake.models.{EOSE, NostrDataEvent, NostrEvent, NostrFilter}
import com.zoomin.earth.datalake.parser.Parser
import io.circe.parser.parse
import com.zoomin.earth.datalake.nostr.NostrSerDesContext.given
import org.typelevel.log4cats.Logger

given nostrParser: Parser[NostrFilter] with
  type In  = String
  type Out = Option[NostrEvent]

  def parse[NostrFilter](message: In)(using logger: Logger[IO]): Out =
    io.circe.parser.parse(message) match
      case Left(value) =>
        logger.error(s"we failed parsing with the error: ${value.getMessage}")
        None

      case Right(json) =>
        json.asArray.flatMap { arr =>
          if arr.size >= 3 && arr.head.asString.contains("EVENT") then
            arr(2).as[NostrDataEvent] match
              case Left(value) =>
                logger.error(
                  s"Failed to parse message or invalid format with err: ${value.getMessage}"
                )
                None
              case Right(value) =>
                Some(value)
          else if arr.head.asString.contains("EOSE") then
            json.as[EOSE] match
              case Left(value) =>
                logger.error(
                  s"Failed to parse message or invalid format with err: ${value.getMessage}, original message: ${arr
                      .toString()} ${arr(1)}"
                )
                None
              case Right(value) =>
                Some(value)
          else
            logger.error(s"we cannot parse this event: ${arr.toString()}")
            None
        }
