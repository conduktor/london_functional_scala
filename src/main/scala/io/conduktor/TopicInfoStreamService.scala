package io.conduktor

import io.conduktor.KafkaService.{Partition, RecordCount, ReplicationFactor, Spread, TopicName, TopicSize}
import io.conduktor.TopicInfoStreamService.Info
import zio.{ZIO, ZLayer}
import zio.stream.ZStream

trait TopicInfoStreamService {

  def streamInfos: ZStream[Any, Throwable, Info]
}

object TopicInfoStreamService {

  sealed trait Info
  object Info {
    case class Topics(topics: Seq[TopicName])              extends Info
    case class Size(topicName: TopicName, size: TopicSize) extends Info

    case class RecordCountInfo(topicName: TopicName, count: RecordCount) extends Info

    case class PartitionInfo(topicName: TopicName, partition: Partition) extends Info

    case class ReplicationFactorInfo(
      topicName: TopicName,
      replicationFactor: ReplicationFactor,
    ) extends Info

    case class SpreadInfo(topicName: TopicName, spread: Spread) extends Info
  }
}

class TopicInfoStreamServiceLive(kafkaService: KafkaService) extends TopicInfoStreamService {
  def streamInfos: ZStream[Any, Throwable, Info] = ZStream.unwrap(
    for {
      names       <- kafkaService.listTopicNames.map(topicNames => Info.Topics(topicNames))
      size        <- kafkaService.getTopicSize
      brokerCount <- kafkaService.brokerCount
    } yield ZStream[Info](names) ++ ZStream.fromIterable(size.map { case (name, size) =>
      Info.Size(name, size)
    }) ++ ZStream.fromIterable(names.topics).mapZIO { name =>
      kafkaService.recordCount(name).map(Info.RecordCountInfo(name, _))
    } ++ ZStream
      .fromIterable(names.topics)
      .grouped(30)
      .mapConcatZIO { names =>
        kafkaService.describeTopics(names).map { result =>
          result.toList.flatMap { case (topicName, desc) =>
            List(
              Info.PartitionInfo(topicName, Partition(desc.partition.size)),
              Info.ReplicationFactorInfo(
                topicName,
                replicationFactor = desc.replicationFactor,
              ),
              Info.SpreadInfo(
                topicName,
                Spread(
                  desc.partition
                    .flatMap(_._2.aliveReplicas)
                    .toSet
                    .size
                    .toDouble / brokerCount.value
                ),
              ),
            )
          }
        }
      }
  )
}

object TopicInfoStreamServiceLive {
  val layer = ZLayer {
    for {
      kafkaService <- ZIO.service[KafkaService]
    } yield new TopicInfoStreamServiceLive(kafkaService)
  }
}
