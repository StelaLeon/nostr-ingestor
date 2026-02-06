package com.zoomin.earth.datalake.parser

import cats.effect.IO
import org.typelevel.log4cats.Logger

trait Parser[T] {
  type In
  type Out
  def parse[T](event: In)(using logger: Logger[IO]): Out
}

object NostrEventParser {
  def parseWith[T](using p: Parser[T], logger: Logger[IO])(event: p.In): p.Out =
    p.parse[T](event)
}
