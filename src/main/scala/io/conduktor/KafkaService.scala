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

  def getTopicSize: Task[Map[TopicName, TopicSize]]

  def beginningOffsets(
      topicPartitions: Seq[TopicPartition]
  ): Task[Map[TopicPartition, Offset]]

  def endOffsets(
      topicPartitions: Seq[TopicPartition]
  ): Task[Map[TopicPartition, Offset]]

  def brokerCount: Task[BrokerCount]

  def recordCount(topicName: TopicName): Task[RecordCount]

}

object KafkaService {
  case class TopicName(value: String) extends AnyVal

  case class Partition(value: Int) extends AnyVal

  case class TopicPartition(topicName: TopicName, partition: Partition) {
    def toZioKafka: AdminClient.TopicPartition =
      AdminClient.TopicPartition(topicName.value, partition.value)
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

  case class TopicSize(value: Long) extends AnyVal {
    def +(other: TopicSize): TopicSize = TopicSize(value + other.value)
  }

  case class Spread(value: Double) extends AnyVal

  case class BrokerId(value: Int) extends AnyVal

  case class BrokerCount(value: Int) extends AnyVal

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

  implicit class IterableOps[K, V](it: Iterable[(K, V)]) {
    def values: Iterable[V] = it.map { case (_, v) => v }

    def mapValues[T](f: V => T): Iterable[T] = it.map { case (_, v) => f(v) }

    def mapBoth[T, U](keyF: K => T, valueF: V => U): Iterable[(T, U)] = it.map {
      case (k, v) => keyF(k) -> valueF(v)
    }

    def flatMapValues[T](f: V => Iterable[T]): Iterable[T] = it.flatMap {
      case (_, v) => f(v)
    }
  }

  override def getTopicSize: Task[Map[TopicName, TopicSize]] =
    for {
      brokerIds <- adminClient.describeClusterNodes().map(_.map(_.id))
      description <- adminClient.describeLogDirs(brokerIds)
    } yield description.values.flatten
      .flatMapValues(_.replicaInfos)
      .mapBoth(
        topicPartition => TopicPartition.from(topicPartition),
        info => TopicSize(info.size)
      )
      .groupMapReduce { case (topicPartition, _) => topicPartition.topicName } {
        case (_, size) => size
      } { _ + _ }

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
      topicPartitions: Seq[TopicPartition]
  ): Task[Map[TopicPartition, Offset]] =
    offsets(topicPartitions, OffsetSpec.EarliestSpec)

  override def endOffsets(
      topicPartitions: Seq[TopicPartition]
  ): Task[Map[TopicPartition, Offset]] =
    offsets(topicPartitions, OffsetSpec.LatestSpec)

  override def brokerCount: Task[BrokerCount] =
    adminClient.describeClusterNodes().map { nodes =>
      BrokerCount(nodes.length)
    }

  override def recordCount(
      topicName: TopicName
  ): Task[RecordCount] =
    describeTopics(List(topicName))
      .map(_.values.headOption)
      .flatMap(
        _.map { details =>
          val topicPartitions =
            details.partition.keySet.map(TopicPartition(topicName, _)).toSeq
          for {
            beginningOffsets <- beginningOffsets(topicPartitions)
            endOffsets <- endOffsets(topicPartitions)
          } yield RecordCount(
            endOffsets.values
              .map(_.value)
              .sum - beginningOffsets.values.map(_.value).sum
          )
        }.getOrElse(
          ZIO.fail(
            new RuntimeException("Should never happen: no partition for topic")
          )
        )
      )
}

object KafkaServiceLive {
  val layer = ZLayer {
    for {
      adminClient <- ZIO.service[AdminClient]
    } yield new KafkaServiceLive(adminClient)
  }
}
