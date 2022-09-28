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

  def describeLogDirs(brokerId: BrokerId): Task[Map[TopicPartition, TopicSize]]

  def offsets(
      topicPartition: Seq[TopicPartition]
  ): Task[Map[TopicPartition, Offsets]]
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

  override def describeLogDirs(
      brokerId: BrokerId
  ): Task[Map[TopicPartition, TopicSize]] = ???

  def offsets2(topicPartition: TopicPartition): Task[Offset] =
    adminClient
      .listOffsets(Map(topicPartition.toZioKafka -> OffsetSpec.EarliestSpec))
      .map { offsets =>
        Offset(offsets.values.head.offset)
      }

  override def offsets(
      topicPartition: Seq[TopicPartition]
  ): Task[Map[TopicPartition, Offsets]] =
    adminClient
      .listOffsets(topicPartition.flatMap { topicPartition =>
        val zioKafkaTopicPartition = topicPartition.toZioKafka
        Seq(
          zioKafkaTopicPartition -> OffsetSpec.EarliestSpec
          //zioKafkaTopicPartition -> OffsetSpec.LatestSpec
        )
      }.toMap)
      .map { offsets =>
        offsets.groupBy(_._1).map { case (kafkaTopicPartition, offsetMap) =>
          val offsets = offsetMap.values.toSeq match {
            case Seq(first, last) if first.offset < last.offset =>
              Offsets(Offset(first.offset), Offset(last.offset))
            case Seq(first, last) =>
              Offsets(Offset(last.offset), Offset(first.offset))
            case other =>
              throw new RuntimeException(
                other.toString()
              ) //TODO: rework that shit
          }
          TopicPartition.from(kafkaTopicPartition) -> offsets
        }
      }
}

object KafkaServiceLive {
  val layer = ZLayer {
    for {
      adminClient <- ZIO.service[AdminClient]
    } yield new KafkaServiceLive(adminClient)
  }
}
