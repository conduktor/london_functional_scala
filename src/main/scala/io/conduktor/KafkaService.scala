package io.conduktor

import io.conduktor.KafkaService._
import zio.kafka.admin.AdminClient
import zio.kafka.admin.AdminClient.{Node, OffsetSpec, TopicPartitionInfo}
import zio.{Task, ZIO, ZLayer}

trait KafkaService {
  def listTopicNames: Task[Seq[TopicName]]

  def describeTopics(
                      topicNames: Seq[TopicName]
                    ): Task[Map[TopicName, TopicDescription]]

  def getTopicSize(brokerId: BrokerId): Task[Map[TopicPartition, TopicSize]]

  def beginningOffsets(
                        topicPartition: Seq[TopicPartition]
                      ): Task[Map[TopicPartition, Offset]]

  def endOffsets(
                  topicPartition: Seq[TopicPartition]
                ): Task[Map[TopicPartition, Offset]]
}

object KafkaService {
  case class TopicName(value: String) extends AnyVal

  case class Partition(value: Int) extends AnyVal

  case class TopicPartition(topic: TopicName, partition: Partition) {
    def toZioKafka: AdminClient.TopicPartition =
      AdminClient.TopicPartition(topic.value, partition.value)
  }

  object TopicPartition {
    def from(topicPartition: AdminClient.TopicPartition): TopicPartition =
      TopicPartition(
        TopicName(topicPartition.name),
        Partition(topicPartition.partition)
      )
  }

  case class RecordCount(value: Long) extends AnyVal

  case class Offsets(beginOffset: Offset, endOffset: Offset)

  case class TopicSize(value: Long) extends AnyVal

  case class Spread(value: Double) extends AnyVal

  case class BrokerId(value: Int) extends AnyVal

  object BrokerId {
    def from(node: Node): BrokerId = BrokerId(node.id)
  }

  case class PartitionInfo(
                            leader: Option[BrokerId],
                            aliveReplicas: Seq[BrokerId]
                          )

  object PartitionInfo {
    def from(partition: TopicPartitionInfo): PartitionInfo =
      PartitionInfo(
        partition.leader.map(BrokerId.from),
        partition.isr.map(BrokerId.from)
      )
  }

  case class TopicDescription(
                               partition: Map[Partition, PartitionInfo],
                               replicationFactor: Int
                             )

  object TopicDescription {
    def from(topicDescription: AdminClient.TopicDescription): TopicDescription =
      TopicDescription(
        topicDescription.partitions.map { partition =>
          Partition(partition.partition) -> PartitionInfo.from(partition)
        }.toMap,
        topicDescription.partitions.map(_.replicas.length).head
      ) //TODO: rework head?

  }

  case class Offset(value: Long)
}

class KafkaServiceLive(adminClient: AdminClient) extends KafkaService {
  override def listTopicNames: Task[Seq[TopicName]] =
    adminClient
      .listTopics()
      .map(_.values.map(listing => TopicName(listing.name)).toList)

  override def describeTopics(
                               topicNames: Seq[TopicName]
                             ): Task[Map[TopicName, TopicDescription]] = {
    adminClient
      .describeTopics(topicNames.map(_.value))
      .map(topicDescription =>
        topicDescription.map { case (topicName, topicDescription) =>
          TopicName(topicName) -> TopicDescription.from(topicDescription)
        }
      )
  }

  override def getTopicSize(
                             brokerId: BrokerId
                           ): Task[Map[TopicPartition, TopicSize]] = {
    for {
      thing <- adminClient.describeLogDirs(brokerId.value :: Nil)
      somethingelse: Iterable[(String, AdminClient.LogDirDescription)] = thing.values.flatten
      replicaInfos = somethingelse.flatMap { case (topic, description) => description.replicaInfos }
    } yield replicaInfos.map { case (partition, info) => TopicPartition.from(partition) -> TopicSize(info.size) }
      .toMap
  }

  private def offsets(
                       topicPartition: Seq[TopicPartition],
                       offsetSpec: OffsetSpec
                     ): Task[Map[TopicPartition, Offset]] =
    adminClient
      .listOffsets(topicPartition.map { topicPartition =>
        topicPartition.toZioKafka -> offsetSpec
      }.toMap)
      .map { offsets =>
        offsets.map { case (kafkaTopicPartition, offset) =>
          TopicPartition.from(kafkaTopicPartition) -> Offset(offset.offset)
        }
      }

  override def beginningOffsets(
                                 topicPartition: Seq[TopicPartition]
                               ): Task[Map[TopicPartition, Offset]] =
    offsets(topicPartition, OffsetSpec.EarliestSpec)

  override def endOffsets(
                           topicPartition: Seq[TopicPartition]
                         ): Task[Map[TopicPartition, Offset]] =
    offsets(topicPartition, OffsetSpec.LatestSpec)
}

object KafkaServiceLive {
  val layer = ZLayer {
    for {
      adminClient <- ZIO.service[AdminClient]
    } yield new KafkaServiceLive(adminClient)
  }
}
