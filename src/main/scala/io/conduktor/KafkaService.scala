package io.conduktor

import io.conduktor.KafkaService.{BrokerId, Offset, Partition, PartitionInfo, TopicDescription, TopicName, TopicPartition, TopicSize}
import zio.kafka.admin.AdminClient
import zio.{Task, ZIO, ZLayer}

trait KafkaService {
  def listTopicNames: Task[Seq[TopicName]]

  def describeTopics(topicNames: Seq[TopicName]): Task[Seq[TopicDescription]]

  def describeLogDirs(brokerId: BrokerId): Task[Map[TopicPartition, TopicSize]]

  def beginOffset(topicPartition: TopicPartition): Task[Offset]

  def endOffset(topicPartition: TopicPartition): Task[Offset]
}

object KafkaService {
  case class TopicName(value: String) extends AnyVal

  case class Partition(value: Int) extends AnyVal

  case class TopicPartition(topic: TopicName, partition: Partition)

  case class RecordCount(value: Long) extends AnyVal

  case class TopicSize(value: Long) extends AnyVal

  case class Spread(value: Double) extends AnyVal

  case class BrokerId(value: String) extends AnyVal

  case class PartitionInfo(leader: Option[BrokerId], aliveReplicas: Seq[BrokerId])

  case class TopicDescription(partition: Map[Partition, PartitionInfo], replicationFactor: Int)

  case class Offset(value: Long)
}

class KafkaServiceLive(adminClient: AdminClient) extends KafkaService {
  override def listTopicNames: Task[Seq[TopicName]] =
    adminClient
      .listTopics()
      .map(_.values.map(listing => TopicName(listing.name)).toList)

  override def describeTopics(topicNames: Seq[TopicName]): Task[Seq[TopicDescription]] =
    adminClient.describeTopics(topicNames.map(_.value))
      .map(
        _.values.map(description => {
          val info: Map[Partition, PartitionInfo] = description.partitions.map(partitionInfo => {
            Partition(partitionInfo.partition) ->
              PartitionInfo(
                leader = partitionInfo.leader.flatMap(_.host).map(BrokerId.apply),
                aliveReplicas = partitionInfo.replicas.flatMap(_.host).map(BrokerId.apply))
          }).toMap
          TopicDescription(partition = info, replicationFactor = 5)
        }

        ).toList
      )

  override def describeLogDirs(brokerId: BrokerId): Task[Map[TopicPartition, TopicSize]] = ???

  override def beginOffset(topicPartition: TopicPartition): Task[Offset] = ???

  override def endOffset(topicPartition: TopicPartition): Task[Offset] = ???
}

object KafkaServiceLive {
  val layer = ZLayer {
    for {
      adminClient <- ZIO.service[AdminClient]
    } yield new KafkaServiceLive(adminClient)
  }
}