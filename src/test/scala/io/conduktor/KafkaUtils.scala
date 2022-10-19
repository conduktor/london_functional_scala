package io.conduktor

import com.dimafeng.testcontainers.KafkaContainer
import io.conduktor.KafkaService.TopicName
import zio.kafka.admin.{AdminClient, AdminClientSettings}
import zio.kafka.admin.AdminClient.NewTopic
import zio.{URIO, ZIO, ZLayer}
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serializer

object KafkaUtils {
  def produce(
    topic: TopicName,
    key: String,
    value: String,
  ): ZIO[Producer, Throwable, Unit] =
    ZIO.serviceWithZIO[Producer](
      _.produce(
        topic.value,
        key = key,
        value = value,
        keySerializer = Serializer.string,
        valueSerializer = Serializer.string,
      ).unit
    )

  def createTopic(
    name: TopicName,
    numPartition: Int = 3,
    replicationFactor: Short = 1,
  ): URIO[AdminClient, Unit] = ZIO
    .serviceWithZIO[AdminClient](
      _.createTopic(
        NewTopic(
          name = name.value,
          numPartitions = numPartition,
          replicationFactor = replicationFactor,
        )
      )
    )
    .orDie

  def createTopic(newTopic: NewTopic): URIO[AdminClient, Unit] = ZIO
    .serviceWithZIO[AdminClient](
      _.createTopic(newTopic)
    )
    .orDie

  val producerLayer =
    ZLayer.scoped {
      ZIO
        .serviceWith[KafkaContainer](container => ProducerSettings(container.bootstrapServers :: Nil))
        .flatMap(Producer.make)
    }

  val adminClientSettingsLayer = ZLayer {
    ZIO.serviceWith[KafkaContainer] { container =>
      AdminClientSettings(container.bootstrapServers :: Nil)
    }
  }
}
