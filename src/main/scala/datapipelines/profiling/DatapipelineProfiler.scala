package com.zoomin.earth.datalake.datapipelines.profiling

import cats.effect.std.Console
import cats.effect.{Ref, Temporal}
import cats.syntax.all.*
import fs2.{Pipe, Stream}

import scala.concurrent.duration.FiniteDuration

trait DatapipelineProfiler {

  def timingPipe[F[_]: Temporal: Console, A, B](work: A => F[B]): Pipe[F, A, B] =
    _.evalMap { a =>
      for {
        start  <- Temporal[F].realTime
        result <- work(a)
        end    <- Temporal[F].realTime
        _      <- Console[F].println(s"Item took: ${(end - start).toMillis}ms")
      } yield result
    }

  def interArrivalTimingPipe[F[_]: Temporal: Console, A]: Pipe[F, A, A] =
    stream =>
      Stream.eval(Ref[F].of(none[FiniteDuration])).flatMap { lastTimeRef =>
        stream.evalMap { a =>
          for {
            now      <- Temporal[F].realTime
            lastTime <- lastTimeRef.getAndSet(now.some)
            _ <- lastTime match {
              case Some(last) =>
                Console[F].println(s"Time since last item: ${(now - last).toMillis}ms")
              case None =>
                Console[F].println("First item")
            }
          } yield a
        }
      }

}

object DatapipelineProfiler extends DatapipelineProfiler {}
