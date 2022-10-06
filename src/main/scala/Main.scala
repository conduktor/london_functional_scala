import io.conduktor.{KafkaServiceLive, RestEndpoints, RestEndpointsLive}
import zhttp.service.Server
import zio._
import zio.kafka.admin.{AdminClient, AdminClientSettings}

object ZioHttpServer extends ZIOAppDefault {
  // starting the server
  override def run =
    (for {
      _ <- ZIO.debug("starting")
      app <- ZIO.service[RestEndpoints].map(_.app)
      _ <- Server
        .start(8090, app)
        .tapErrorCause(cause => ZIO.logErrorCause(cause))
    } yield ()).exitCode
      .provide(
        RestEndpointsLive.layer,
        KafkaServiceLive.layer,
        AdminClient.live,
        ZLayer.succeed((AdminClientSettings(List("localhost:9092"))))
      )

}
