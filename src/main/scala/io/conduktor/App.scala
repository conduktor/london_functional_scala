package io.conduktor

import io.conduktor.v2.TopicInfoPaginatedStreamServiceLive
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.server.Server
import zio._
import zio.interop.catz._
import zio.kafka.admin.{AdminClient, AdminClientSettings}

object App extends ZIOAppDefault {

  def serverLayer(port: Int): ZLayer[RestEndpoints, Throwable, Server] =
    ZLayer.scoped(for {
      restEndpoints  <- ZIO.service[RestEndpoints]
      serverResource <- ZIO.executor.map { executor =>
                          BlazeServerBuilder[Task]
                            .withExecutionContext(executor.asExecutionContext)
                            .bindHttp(port, "localhost")
                            .withHttpWebSocketApp(restEndpoints.app)
                            .resource
                        }
      serverScoped   <- serverResource.toScopedZIO
    } yield serverScoped)

  // starting the server
  override def run =
    (for {
      _ <- ZIO.debug("starting")
      _ <- ZIO.service[Server]
      _ <- ZIO.never
    } yield ()).exitCode
      .provide(
        serverLayer(8090),
        RestEndpointsLive.layer,
        TopicInfoPaginatedStreamServiceLive.layer,
        KafkaServiceLive.layer,
        TopicInfoStreamServiceLive.layer,
        AdminClient.live,
        ZLayer.succeed(
          AdminClientSettings(
            List("localhost:9092"),
            closeTimeout = 30.seconds,
            properties = Map[String, AnyRef](),
          )
        ),
      )
}
