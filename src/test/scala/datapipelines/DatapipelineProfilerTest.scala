package com.zoomin.earth.datalake.datapipelines

import munit.CatsEffectSuite
import cats.effect.IO
import fs2.Stream

import scala.concurrent.duration.*
import com.zoomin.earth.datalake.datapipelines.profiling.DatapipelineProfiler.*

class DatapipelineProfilerTest extends CatsEffectSuite {

  test("interArrivalTimingPipe should measure time between items") {
    val items = List(1, 2, 3)

    val result = Stream
      .emits(items)
      .evalMap(i => IO.sleep(100.millis).as(i))
      .through(interArrivalTimingPipe[IO, Int])
      .compile
      .toList

    result.map { list =>
      assertEquals(list, items)
    }
  }

  test("timingPipe should measure how long a long computation took") {
    val items = List(1, 2, 3)

    val result = Stream
      .emits(items)
      .through(timingPipe[IO, Int, Int](i => IO.sleep(100.millis).as(i)))
      .compile
      .toList

    result.map { list =>
      assertEquals(list, items)
    }
  }

  test("timingPipe should measure how long a long computation took- exceptions") {
    val items = List(1, 2, 3)

    val result = Stream
      .emits(items)
      .through(timingPipe[IO, Int, Int](i => IO.sleep(100.millis).as(i) >> IO.raiseError(new Exception("Boom!"))))
      .compile
      .toList
      .attempt

    result.map {
      case Left(err) => assertEquals(err.getMessage, "Boom!")
      case Right(_)  => fail("Expected error")
    }
  }
}
