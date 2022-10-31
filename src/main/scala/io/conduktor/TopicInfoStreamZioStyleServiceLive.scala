package io.conduktor

import io.conduktor.KafkaService.{BrokerCount, Partition, RecordCount, TopicDescription, TopicName, TopicPartition, TopicSize}
import io.conduktor.TopicInfoStreamService.Info
import zio.stream.ZStream
import zio.{NonEmptyChunk, Queue, Task, UIO, ZIO, ZLayer}

class TopicInfoStreamZioStyleServiceLive(kafkaService: KafkaService) extends TopicInfoStreamService {
  implicit class QueueOps(queue: Queue[Info]) {
    def sendNames(names: Set[TopicName]): UIO[Unit] =
      queue.offer(Info.Topics(names)).when(names.nonEmpty).unit

    def sendSizes(sizes: Map[TopicName, TopicSize]): UIO[Unit] =
      queue.offerAll(sizes.map { case (name, size) => Info.Size(name, size) }).unit

    def sendSpreadPartitionAndReplicationFactor(brokerCount: BrokerCount)(info: (TopicName, TopicDescription)): UIO[Unit] = {
      val (topicName, topicDescription) = info
      queue
        .offerAll(
          List(
            Info.SpreadInfo(topicName, kafkaService.topicSpread(brokerCount, topicDescription)),
            Info.PartitionInfo(topicName, Partition(topicDescription.partition.size)),
            Info.ReplicationFactorInfo(topicName, topicDescription.replicationFactor),
          )
        )
        .unit
    }

    def sendRecordCount(info: (TopicName, NonEmptyChunk[TopicPartitionAndSize])): UIO[Unit] =
      queue.offer(Info.RecordCountInfo(info._1, RecordCount(info._2.map(_.size).sum))).unit

    val complete: UIO[Unit] = queue.offer(Info.Complete).unit
  }

  case class TopicPartitionAndSize(topicPartition: TopicPartition, size: Long)

  implicit class KafkaServiceOps(kafkaService: KafkaService) {
    def countRecordForPartitions(topicPartitions: Seq[TopicPartition]): Task[List[TopicPartitionAndSize]] =
      kafkaService.beginningOffsets(topicPartitions).zipWithPar(kafkaService.endOffsets(topicPartitions)) {
        case (beginningOffsets, endOffsets) =>
          endOffsets
            .map { case (topicPartition, endOffset) =>
              TopicPartitionAndSize(
                topicPartition,
                size = endOffset.value - beginningOffsets(topicPartition).value,
              )
            }
            .toList
            .sortBy(_.topicPartition)
      }
  }

  def extractTopicPartitions(topicName: TopicName, topicDescription: TopicDescription): List[TopicPartition] =
    topicDescription.partition.keys.toList.map(TopicPartition(topicName, _))

  def streamThings(generateInfo: Queue[Info] => Task[Unit]) =
    ZStream.unwrap(for {
      queue <- Queue.unbounded[Info]
      _     <- generateInfo(queue).fork
    } yield ZStream.fromQueue(queue, maxChunkSize = 1))

  def describeTopics(names: Seq[TopicName]) = ZStream
    .fromIterable(names)
    .grouped(30)
    .mapConcatZIO(kafkaService.describeTopics)

  def countRecordForPartitions(
    info: ZStream[Any, Throwable, (TopicName, TopicDescription)]
  ): ZStream[Any, Throwable, (TopicName, NonEmptyChunk[TopicPartitionAndSize])] =
    info
      .mapConcat((extractTopicPartitions _).tupled)
      .grouped(30)
      .mapConcatZIO { topicPartitions =>
        kafkaService.countRecordForPartitions(topicPartitions)
      }
      .groupAdjacentBy(_.topicPartition.topicName)

  override def streamInfos: ZStream[Any, Throwable, TopicInfoStreamService.Info] =
    streamThings { queue =>
      for {
        names <- kafkaService.listTopicNames.map(_.toSet).tap(queue.sendNames)

        brokerIds  <- kafkaService.getBrokerIds
        brokerCount = BrokerCount(brokerIds.length)

        _ <- kafkaService.getTopicSize(brokerIds).forEachZIO(queue.sendSizes)

        _ <- describeTopics(names.toList)
               .tap(queue.sendSpreadPartitionAndReplicationFactor(brokerCount))
               .viaFunction(countRecordForPartitions)
               .tap(queue.sendRecordCount)
               .runDrain

        _ <- queue.complete
      } yield ()
    }
}

object TopicInfoStreamZioStyleServiceLive {
  val layer = ZLayer {
    for {
      kafkaService <- ZIO.service[KafkaService]
    } yield new TopicInfoStreamZioStyleServiceLive(kafkaService)
  }
}
