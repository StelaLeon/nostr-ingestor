package com.zoomin.earth.datalake.config

import cats.effect.Sync
import cats.syntax.all.*
import pureconfig.*
import pureconfig.generic.derivation.*
import pureconfig.module.catseffect.*
import scala.concurrent.duration.FiniteDuration
import pureconfig.module.catseffect.syntax.*
import java.time.{LocalDate, ZoneOffset}

object ConfigLoader {

  given timestampReader: ConfigReader[Long] = ConfigReader[String].map { s =>
    LocalDate
      .parse(s)
      .atStartOfDay()
      .toEpochSecond(ZoneOffset.UTC)
  }

  given ConfigReader[List[String]] = ConfigReader[String]
    .map { str =>
      if str.trim.startsWith("[") then {
        str.trim
          .stripPrefix("[")
          .stripSuffix("]")
          .split(",")
          .map(_.trim.stripPrefix("\"").stripSuffix("\""))
          .toList
      } else {
        str.split(",").map(_.trim).toList
      }
    }
    .orElse(ConfigReader.derived[List[String]])

  given ConfigReader[NostrRelayConfig] = ConfigReader.derived
  given ConfigReader[BigQueryConfig]   = ConfigReader.derived
  given ConfigReader[StreamConfig]     = ConfigReader.derived
  given ConfigReader[PipelineConfig]   = ConfigReader.derived

  def load[F[_]: Sync]: F[PipelineConfig] =
    ConfigSource.default
      .at("nostr")
      .loadF[F, PipelineConfig]()
      .adaptError { case e =>
        new RuntimeException(s"Failed to load configuration: ${e.getMessage}", e)
      }

}
