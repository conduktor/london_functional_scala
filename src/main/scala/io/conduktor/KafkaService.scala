package io.conduktor

import io.conduktor.KafkaService._
import zio.kafka.admin.AdminClient
import zio.kafka.admin.AdminClient.{ListTopicsOptions, Node, OffsetSpec, TopicPartitionInfo}
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

  def topicSpread(topicName: TopicName): Task[Spread]

  def topicSpread(numBroker: BrokerCount, topicDescription: TopicDescription): Spread

  val getBrokerIds: Task[List[BrokerId]]

  def getTopicSize(brokerIds: Seq[BrokerId]): Task[Map[TopicName, TopicSize]]
}

object KafkaService {

  case class TopicName(value: String) extends AnyVal
  object TopicName {
    implicit val ordering: Ordering[TopicName] = (x: TopicName, y: TopicName) => implicitly[Ordering[String]].compare(x.value, y.value)
  }

  case class Partition(value: Int) extends AnyVal

  case class TopicPartition(topicName: TopicName, partition: Partition) {
    def toZioKafka: AdminClient.TopicPartition =
      AdminClient.TopicPartition(topicName.value, partition.value)
  }

  object TopicPartition {
    def from(topicPartition: AdminClient.TopicPartition): TopicPartition =
      TopicPartition(
        TopicName(topicPartition.name),
        Partition(topicPartition.partition),
      )

    implicit val ordering: Ordering[TopicPartition] = (x: TopicPartition, y: TopicPartition) =>
      implicitly[Ordering[(String, Int)]].compare((x.topicName.value, x.partition.value), (y.topicName.value, y.partition.value))
  }

  case class RecordCount(value: Long) extends AnyVal

  case class ReplicationFactor(value: Int) extends AnyVal

  case class PartitionCount(value: Int) extends AnyVal

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
    aliveReplicas: Seq[BrokerId],
  )

  object PartitionInfo {
    def from(partition: TopicPartitionInfo): PartitionInfo =
      PartitionInfo(
        partition.leader.map(BrokerId.from),
        partition.isr.map(BrokerId.from),
      )
  }

  case class TopicDescription(
    partition: Map[Partition, PartitionInfo],
    replicationFactor: ReplicationFactor,
  )

  object TopicDescription {
    def from(topicDescription: AdminClient.TopicDescription): TopicDescription =
      TopicDescription(
        topicDescription.partitions.map { partition =>
          Partition(partition.partition) -> PartitionInfo.from(partition)
        }.toMap,
        ReplicationFactor(
          topicDescription.partitions.map(_.replicas.length).head
        ),
      )

  }

  case class Offset(value: Long)
}

class KafkaServiceLive(adminClient: AdminClient) extends KafkaService {
  override def listTopicNames: Task[Seq[TopicName]] =
    adminClient
      .listTopics(Some(ListTopicsOptions(listInternal = true, timeout = None)))
      .map(_.values.map(listing => TopicName(listing.name)).toList)

  override def describeTopics(
    topicNames: Seq[TopicName]
  ): Task[Map[TopicName, TopicDescription]] =
    adminClient
      .describeTopics(topicNames.map(_.value))
      .map(topicDescription =>
        topicDescription.map { case (topicName, topicDescription) =>
          TopicName(topicName) -> TopicDescription.from(topicDescription)
        }
      )

  implicit class IterableOps[K, V](it: Iterable[(K, V)]) {
    def values: Iterable[V] = it.map { case (_, v) => v }

    def mapValues[T](f: V => T): Iterable[T] = it.map { case (_, v) => f(v) }

    def mapBoth[T, U](keyF: K => T, valueF: V => U): Iterable[(T, U)] = it.map { case (k, v) =>
      keyF(k) -> valueF(v)
    }

    def flatMapValues[T](f: V => Iterable[T]): Iterable[T] = it.flatMap { case (_, v) =>
      f(v)
    }
  }

  override val getBrokerIds: ZIO[Any, Throwable, List[BrokerId]] =
    adminClient.describeClusterNodes().map(_.map(broker => BrokerId(broker.id)))

  override def getTopicSize: Task[Map[TopicName, TopicSize]] =
    getBrokerIds.flatMap(getTopicSize(_))

  override def getTopicSize(brokerIds: Seq[BrokerId]): Task[Map[TopicName, TopicSize]] =
    adminClient.describeLogDirs(brokerIds.map(_.value)).map { description =>
      description.values.flatten
        .flatMapValues(_.replicaInfos)
        .mapBoth(
          topicPartition => TopicPartition.from(topicPartition),
          info => TopicSize(info.size),
        )
        .groupMapReduce { case (topicPartition, _) => topicPartition.topicName } { case (_, size) =>
          size
        }(_ + _)
    }

  override def topicSpread(topicName: TopicName): Task[Spread] =
    for {
      numBrokers  <- brokerCount
      description <- describeTopics(Seq(topicName)).map(topics => topics.head._2)
    } yield topicSpread(numBrokers, description)

  override def topicSpread(numBroker: BrokerCount, topicDescription: TopicDescription): Spread = Spread(
    topicDescription.partition.values.flatMap(_.aliveReplicas).toSet.size.toDouble / numBroker.value.toDouble
  )

  private def offsets(
    topicPartition: Seq[TopicPartition],
    offsetSpec: OffsetSpec,
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
    getBrokerIds.map { brokerId =>
      BrokerCount(brokerId.length)
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
            endOffsets       <- endOffsets(topicPartitions)
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
