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
            List("insane-europe-kafka-0-conduktor-09ba.aivencloud.com:21661"),
            closeTimeout = 30.seconds,
            properties = Map[String, AnyRef](
              "security.protocol"           -> "SASL_SSL",
              "sasl.mechanism"              -> "SCRAM-SHA-512",
              "sasl.jaas.config"            -> "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"avnadmin\" password=\"AVNS_SkzphCDGUVTdTF-TCGu\";",
              "ssl.truststore.type"         -> "PEM",
              "ssl.truststore.certificates" -> "-----BEGIN CERTIFICATE----- MIIEQTCCAqmgAwIBAgIUbfoZ+cSDltSWuJj7727IYjwYUOwwDQYJKoZIhvcNAQEMBQAwOjE4MDYGA1UEAwwvNzAzMDg5NzYtNjU2YS00ZDkzLThkZGQtMzJiZGNmYmM2MGY2IFByb2plY3QgQ0EwHhcNMjIwNTA1MTAxODQ4WhcNMzIwNTAyMTAxODQ4WjA6MTgwNgYDVQQDDC83MDMwODk3Ni02NTZhLTRkOTMtOGRkZC0zMmJkY2ZiYzYwZjYgUHJvamVjdCBDQTCCAaIwDQYJKoZIhvcNAQEBBQADggGPADCCAYoCggGBALdsDL60ycJF+NyJ4OXHC2Xd+23oL7ycK/II4BhmpTtU7bIjP4ImlEvBBlbf/u8Fw7NMImiz+t9goXftBuc38NOKUv+/jZp2zTa9POjn6Brp1mhoP05bOe2/k5kk5NNpB4Rgjm4bD2GPkKgwDzm2IfMiGM1YcydyofTkv88CqtgOSgMRvtoB1PLzRV06g3QLbb0GvBCKyykXDcS5jK48MKJSeTfLirkPpHMkcA2M1wcEdQ0UJssVnAdlG6yrqf7TldyC99lGOzuT/FtudsLCvxEnt+QMDSh5umX6PKRRJIrywozQPrKNRyLnrLQMJRNl8yJy4N5NEfzMj6C4fXgNcsz+CGrlDYw2KkmL40xkhdt896yT5G4aruMDPLRNkxOmecUJqDXaArFu+5L9SUDBX3Mrfr26+42ICVI8yX5cjjB4aCG9rLorX3rlKvSt2I1tlMPgUVloEdis4HbxG3sSgPp9N6UwudFe+FSlt98eOIdcEZD+SyLfK4WPpBprdU82sQIDAQABoz8wPTAdBgNVHQ4EFgQUhIYI0Z5HgBsxQF9RC2CROrfwEB8wDwYDVR0TBAgwBgEB/wIBADALBgNVHQ8EBAMCAQYwDQYJKoZIhvcNAQEMBQADggGBACyNUIBCi3IAicaj1ohTV4qEUWaYmraTTazu4Npm+pt5zGRgx9GiNhT1XQuVIw4BvnFQjZY0H8pNPOKd2SnmJFcbWMmt4KV9MhneA2VPmbzK13IPkFdD80Vq+xTdLVGvTreZCN1E1YG3OiEmwL3zW3YKg1cNk9c08rISCFq4iny1MVLEpyqj2xoUNywvd8HBgpL8bC4VhOCOGcTGl4TA6qgD7Gem8TFDv1QYcDyes0RkULK9ojncLlUp0QY1UsumiUUmk54b30PlwJkSc3tD4PG5DR4OKIBxj2dxdEsSc6mT5Ei6HfsO1LkZgfLFoL4P/0mU8CrrvjUT3vRvCdsmSBX2dBN+O7BqMVK7bFcA4T3mRhqbyCkRwLNR8bQtDAhe+BKHx6VTKJHpJy8yRDYea3HfwDsmSXa0JmoRxEaJzIlHVgZJE0y3xJ93WotmQREGbywddZt9KMMU0RT8fAmh8ScpAqO0kcN/zOzWeQ3zaBlpi5i7JpnCbZQgex5m/s9N5w== -----END CERTIFICATE-----",
            ),
          )
        ),
      )
}
