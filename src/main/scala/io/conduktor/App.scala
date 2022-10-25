package io.conduktor

import org.http4s.HttpApp
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.server.Server
import zio._
import zio.interop.catz._
import zio.kafka.admin.{AdminClient, AdminClientSettings}

object App extends ZIOAppDefault {

  def serverLayer(port: Int): ZLayer[HttpApp[Task], Throwable, Server] =
    ZLayer.scoped(for {
      app            <- ZIO.service[HttpApp[Task]]
      serverResource <- ZIO.executor.map { executor =>
                          BlazeServerBuilder[Task]
                            .withExecutionContext(executor.asExecutionContext)
                            .bindHttp(port, "localhost")
                            .withHttpApp(app)
                            .resource
                        }
      serverScoped   <- serverResource.toScopedZIO
    } yield serverScoped)

  // starting the server
  override def run =
    (for {
      _ <- ZIO.debug("starting")
      _ <- ZIO.service[Server]
    } yield ()).exitCode
      .provide(
        serverLayer(8090),
        RestEndpointsLive.layer,
        KafkaServiceLive.layer,
        AdminClient.live,
        ZLayer.succeed(AdminClientSettings(List("localhost:9092"))),
      )

}
