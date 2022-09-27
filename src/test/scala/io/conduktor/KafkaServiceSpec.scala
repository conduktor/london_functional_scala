package io.conduktor

import com.dimafeng.testcontainers.KafkaContainer
import io.conduktor.KafkaService.TopicName
import zio._
import zio.kafka.admin.{AdminClient, AdminClientSettings}
import zio.kafka.admin.AdminClient.NewTopic
import zio.test._

object KafkaTestContainer {
  val kafkaLayer: ZLayer[Any, Nothing, KafkaContainer] =
    ZLayer.scoped {
      ZIO.acquireRelease(ZIO.attemptBlocking {
        val container = KafkaContainer()
        container.start()
        container
      }.orDie)(kafka => ZIO.attemptBlocking(kafka.stop()).orDie)
    }
}

object KafkaAdmin {
  val adminClientSettings = ZLayer {
    ZIO.serviceWith[KafkaContainer](container => AdminClientSettings(container.bootstrapServers :: Nil))
  }

  def createTopic(name: TopicName): URIO[AdminClient, Unit] = ZIO.serviceWithZIO[AdminClient](_.createTopic(NewTopic(name = name.value, numPartitions = 3, replicationFactor = 1))).orDie
}

object KafkaServiceSpec extends ZIOSpecDefault {
  override def spec = suite("KafkaService")(
    test("should list topics") {
      for {
        topics <- ZIO.serviceWithZIO[KafkaService](_.listTopicNames)
      } yield assertTrue(topics.isEmpty)
    },
    test("should list topics") {
      for {
        _ <- KafkaAdmin.createTopic(TopicName("foo"))
        topics <- ZIO.serviceWithZIO[KafkaService](_.listTopicNames)
      } yield assertTrue(topics == Seq(TopicName("foo")))
    }
  ).provide(KafkaTestContainer.kafkaLayer, KafkaServiceLive.layer, AdminClient.live, KafkaAdmin.adminClientSettings)
}
