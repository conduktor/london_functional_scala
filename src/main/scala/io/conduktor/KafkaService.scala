package io.conduktor

import io.conduktor.KafkaService.{BrokerId, Offset, Partition, PartitionInfo, TopicDescription, TopicName, TopicPartition, TopicSize}
import zio.kafka.admin.AdminClient
import zio.kafka.admin.AdminClient.{Node, TopicPartitionInfo}
import zio.{Task, ZIO, ZLayer}

trait KafkaService {
  def listTopicNames: Task[Seq[TopicName]]

  def describeTopics(topicNames: Seq[TopicName]): Task[Map[TopicName, TopicDescription]]

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

  case class BrokerId(value: Int) extends AnyVal
  object BrokerId {
    def apply(node: Node): BrokerId = BrokerId(node.id)
  }

  case class PartitionInfo(leader: Option[BrokerId], aliveReplicas: Seq[BrokerId])
  object PartitionInfo {
    def apply(partition: TopicPartitionInfo): PartitionInfo =
      PartitionInfo(partition.leader.map(BrokerId(_)), partition.isr.map(BrokerId(_)))
  }

  case class TopicDescription(partition: Map[Partition, PartitionInfo], replicationFactor: Int)
  object TopicDescription {
    def apply(topicDescription: AdminClient.TopicDescription): TopicDescription =
      TopicDescription(topicDescription.partitions.map { partition =>
        Partition(partition.partition) -> PartitionInfo(partition)
      }.toMap, topicDescription.partitions.map(_.replicas.length).head) //TODO: rework head?

  }

  case class Offset(value: Long)
}

class KafkaServiceLive(adminClient: AdminClient) extends KafkaService {
  override def listTopicNames: Task[Seq[TopicName]] =
    adminClient
      .listTopics()
      .map(_.values.map(listing => TopicName(listing.name)).toList)

  override def describeTopics(topicNames: Seq[TopicName]): Task[Map[TopicName, TopicDescription]] = {
    //fix that shit
    adminClient.describeTopics(topicNames.map(_.value))
      .map(
        _.values.map(description => {
          val info: Map[Partition, PartitionInfo] = description.partitions.map(partitionInfo => {
            Partition(partitionInfo.partition) ->
              PartitionInfo(
                leader = partitionInfo.leader.map(_.id).map(BrokerId.apply),
                aliveReplicas = partitionInfo.replicas.map(_.id).map(BrokerId.apply))
          }).toMap
          TopicDescription(partition = info, replicationFactor = 5)
        }

        ).toList
      )
    ???
  }

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