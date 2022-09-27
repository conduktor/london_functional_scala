package io.conduktor

import com.dimafeng.testcontainers.KafkaContainer
import io.conduktor.KafkaService.{BrokerId, Partition, PartitionInfo, TopicDescription, TopicName}
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

  def createTopic(name: TopicName, numPartition: Int = 3): URIO[AdminClient, Unit] = ZIO.serviceWithZIO[AdminClient](_.createTopic(NewTopic(name = name.value, numPartitions = numPartition, replicationFactor = 1))).orDie
}

object KafkaServiceSpec extends ZIOSpecDefault {
  override def spec = suite("KafkaService")(
    suite("listTopics")(
      test("should list topics when empty") {
        for {
          topics <- ZIO.serviceWithZIO[KafkaService](_.listTopicNames)
        } yield assertTrue(topics.isEmpty)
      },
      test("should list topics") {
        val topicName = TopicName("foo")
        for {
          _ <- KafkaAdmin.createTopic(topicName)
          topics <- ZIO.serviceWithZIO[KafkaService](_.listTopicNames)
        } yield assertTrue(topics == Seq(topicName))
      }
    ).provide(KafkaTestContainer.kafkaLayer, KafkaServiceLive.layer, AdminClient.live, KafkaAdmin.adminClientSettings),
    suite("describeTopics")(
      test("empty input topic list should return empty result") {
        for {
          result <- ZIO.serviceWithZIO[KafkaService](_.describeTopics(Seq.empty))
        } yield assertTrue(result == Seq.empty)
      },
      test("should properly describe two topics") {
        val topicName1 = TopicName("one")
        val topicName2 = TopicName("two")
        for {
          _ <- KafkaAdmin.createTopic(topicName1)
          _ <- KafkaAdmin.createTopic(topicName2)
          result <- ZIO.serviceWithZIO[KafkaService](_.describeTopics(topicName1 :: topicName2 :: Nil))
        } yield assertTrue(result == List(TopicDescription(partition = Map(Partition(1) -> PartitionInfo(Some(BrokerId("1")), Seq.empty)), replicationFactor = 1)))
      }
    ).provideShared(KafkaTestContainer.kafkaLayer, KafkaServiceLive.layer, AdminClient.live, KafkaAdmin.adminClientSettings)
  )
}
