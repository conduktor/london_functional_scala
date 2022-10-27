package io.conduktor

import io.conduktor.KafkaService.{BrokerCount, Partition, RecordCount, TopicPartition}
import io.conduktor.TopicInfoStreamService.Info
import zio.{Queue, ZIO, ZLayer}
import zio.stream.ZStream

class TopicInfoStreamZioStyleServiceLive(kafkaService: KafkaService) extends TopicInfoStreamService {
  override def streamInfos: ZStream[Any, Throwable, TopicInfoStreamService.Info] = ZStream.unwrap(for {
    queue <- Queue.unbounded[Info]
    result = ZStream.fromQueue(queue, maxChunkSize = 1)
    _     <- (for {
               names      <- kafkaService.listTopicNames
                               .tap(names => queue.offer(Info.Topics(names)).when(names.nonEmpty))
               brokerIds  <- kafkaService.getBrokerIds
               brokerCount = BrokerCount(brokerIds.length)
               _          <- kafkaService.getTopicSize(brokerIds).tap { sizes =>
                               queue.offerAll(sizes.map { case (name, size) => Info.Size(name, size) })
                             }
               _          <- ZStream
                               .fromIterable(names)
                               .grouped(30)
                               .mapZIO { names =>
                                 kafkaService.describeTopics(names)
                               }
                               .mapConcat(_.toList)
                               .tap { case (topicName, topicDescription) =>
                                 queue.offerAll(
                                   List(
                                     Info.SpreadInfo(topicName, kafkaService.topicSpread(brokerCount, topicDescription)),
                                     Info.PartitionInfo(topicName, Partition(topicDescription.partition.size)),
                                     Info.ReplicationFactorInfo(topicName, topicDescription.replicationFactor),
                                   )
                                 )
                               }
                               .mapConcat { case (topicName, topicDescription) =>
                                 topicDescription.partition.keys.toList.map(TopicPartition(topicName, _))
                               }
                               .grouped(30)
                               .mapConcatZIO { partitions =>
                                 kafkaService.beginningOffsets(partitions).zipWithPar(kafkaService.endOffsets(partitions)) {
                                   case (beginningOffsets, endOffsets) =>
                                     endOffsets
                                       .map { case (topicPartition, endOffset) =>
                                         topicPartition -> (endOffset.value - beginningOffsets(topicPartition).value)
                                       }
                                       .toList
                                       .sortBy(_._1)
                                 }
                               }
                               .groupAdjacentBy(_._1.topicName)
                               .tap { case (topicName, recordPerPartitions) =>
                                 val result = recordPerPartitions.map(_._2).sum
                                 queue.offer(Info.RecordCountInfo(topicName = topicName, RecordCount(result)))
                               }
                               .runDrain
               _          <- queue.offer(Info.Complete)
             } yield ()).fork
  } yield result)
}

object TopicInfoStreamZioStyleServiceLive {
  val layer = ZLayer {
    for {
      kafkaService <- ZIO.service[KafkaService]
    } yield new TopicInfoStreamZioStyleServiceLive(kafkaService)
  }
}
