package com.zoomin.earth.datalake.backends.websocket

import cats.effect.kernel.Concurrent
import cats.syntax.all.*
import sttp.capabilities.WebSockets
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.{SttpBackend, asWebSocket, basicRequest}
import sttp.model.Uri
import sttp.ws.{WebSocket, WebSocketFrame}
import com.zoomin.earth.datalake.annotations.doc

@doc(
""" Abstraction over WebSocket operations to enable testing without real WebSocket connections.
    |This trait exposes only the operations needed by the application"""
  .stripMargin)
trait WebSocketOps[F[_]] {

  def send(frame: WebSocketFrame): F[Unit]

  def receive(): F[WebSocketFrame]
}

object WebSocketOps {
  @doc("""
        Creates a WebSocketOps instance from an STTP WebSocket.
        Used in production to wrap the real WebSocket connection.
  """.stripMargin)
  def fromSttpWebSocket[F[_]](ws: WebSocket[F]): WebSocketOps[F] =
    new WebSocketOps[F] {
      override def send(frame: WebSocketFrame): F[Unit] = ws.send(frame)
      override def receive(): F[WebSocketFrame]         = ws.receive()
    }

}

@doc("Abstraction over WebSocket connection establishment.")
trait WebSocketClient[F[_]] {

  @doc(""" Establishes a WebSocket connection to the given URL.
         |@param relayUrl The WebSocket URL to connect to
         |@return Either an error message (Left) or a WebSocketOps instance (Right)""")
  def withConnection[A](relayUrl: String)(use: WebSocketOps[F] => F[A]): F[Either[String, A]]
}

object WebSocketClient {

  @doc("""
   |Creates a production WebSocketClient that uses a real STTP backend.
   |@param backend The STTP backend capable of WebSocket connections
   |@return A WebSocketClient instance that can establish real connections""".stripMargin)
  def apply[F[_]: Concurrent](
    backend: SttpBackend[F, Fs2Streams[F] & WebSockets]
  ): WebSocketClient[F] = new WebSocketClient[F] {

    def withConnection[A](relayUrl: String)(use: WebSocketOps[F] => F[A]): F[Either[String, A]] =
      basicRequest
        .get(Uri.unsafeParse(relayUrl))
        .response(asWebSocket[F, A] { ws =>
          use(WebSocketOps.fromSttpWebSocket(ws))
        })
        .send(backend)
        .map(_.body)

  }

}
