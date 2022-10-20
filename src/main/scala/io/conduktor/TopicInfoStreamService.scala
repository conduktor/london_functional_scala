package io.conduktor

import io.conduktor.KafkaService.{
  BrokerCount,
  BrokerId,
  Offset,
  Partition,
  RecordCount,
  ReplicationFactor,
  Spread,
  TopicDescription,
  TopicName,
  TopicPartition,
  TopicSize,
}
import io.conduktor.TopicInfoStreamService.Info
import zio.{Queue, ZIO, ZLayer}
import zio.stream.{Stream, UStream, ZSink, ZStream}

import scala.collection.immutable
import scala.collection.immutable.TreeMap

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
  def streamInfosOld: ZStream[Any, Throwable, Info] = ZStream.unwrap(
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

  def streamInfos = ZStream
    .fromZIO(for {
      queue <- Queue.unbounded[Command]
      _     <- queue.offer(Command.Init)
      stream =
        ZStream
          .fromQueue(queue)
          .collectWhileZIO {
            case e: Command.Step          =>
              ZIO.succeed(e)
            case Command.StopWithError(t) =>
              ZIO.fail(t)
            //Order.Stop not handle so we stop the stream when it occur
          }
          .mapAccumZIO(State.empty) { (state, message: Command.Step) =>
            val orderResult = message.run(oldInnerState = state)
            orderResult.nextCommand
              .catchAll { e =>
                ZStream.succeed(Command.StopWithError(e))
              }
              .run(ZSink.fromQueue(queue))
              .fork
              .as(orderResult.newInnerState -> computeOutput(state, orderResult.newInnerState))
          }
          .flatten
    } yield stream)
    .flatten

  private def computeOutput(oldState: InnerState, newState: InnerState): UStream[Info] =
    ZStream.fromIterable {
      val newNames = newState.topics.keySet -- oldState.topics.keySet
      (if (newNames.nonEmpty) List(Info.Topics(newNames.toList)) else List.empty) ++ newState.topics.flatMap {
        case (topicName, newTopicData) =>
          val oldTopicData = oldState.topics.get(topicName).getOrElse(TopicData.empty)
          val size         = newTopicData.size.filter(!oldTopicData.size.contains(_)).map(size => Info.Size(topicName, size))

          val recordCountInfo =
            newTopicData.recordCount
              .filter(!oldTopicData.recordCount.contains(_))
              .map(recordCount => Info.RecordCountInfo(topicName, recordCount))

          val partitionInfo =
            newTopicData.partition.filter(!oldTopicData.partition.contains(_)).map(partition => Info.PartitionInfo(topicName, partition))

          val replicationFactorInfo =
            newTopicData.replicationFactor
              .filter(!oldTopicData.replicationFactor.contains(_))
              .map(replicationFactor => Info.ReplicationFactorInfo(topicName, replicationFactor))

          val spreadInfo =
            newTopicData
              .spread(newState.brokerCount)
              .filter(!oldTopicData.spread(oldState.brokerCount).contains(_))
              .map(spread => Info.SpreadInfo(topicName, spread))

          List(
            size,
            recordCountInfo,
            partitionInfo,
            replicationFactorInfo,
            spreadInfo,
          ).flatten
      }
    }

  val topicGroupSize = 30
  private final case class TopicData(
    describeTopic: Option[TopicDescription],
    beginOffset: Option[Map[Partition, Offset]],
    endOffset: Option[Map[Partition, Offset]],
    size: Option[TopicSize],
  ) {
    def recordCount: Option[RecordCount]                         =
      endOffset.zip(beginOffset).map { case (end, begin) =>
        val sumEndOfsets    = end.values.map(_.value).sum
        val sumStartOffsets = begin.values.map(_.value).sum
        RecordCount(sumEndOfsets - sumStartOffsets)
      }
    def partition: Option[Partition]                             = describeTopic.map(_.partition.size).map(Partition)
    def replicationFactor: Option[ReplicationFactor]             = describeTopic.map(_.replicationFactor)
    def spread(brokerCount: Option[BrokerCount]): Option[Spread] =
      describeTopic.zip(brokerCount).map { case (describe, brokerCount) => kafkaService.topicSpread(brokerCount, describe) }
  }
  private object TopicData {
    def empty: TopicData = TopicData(describeTopic = None, beginOffset = None, endOffset = None, size = None)
  }

  private case class InnerState(topics: TreeMap[TopicName, TopicData], brokerCount: Option[BrokerCount])

  private object State {
    def empty: InnerState = InnerState(topics = TreeMap.empty, brokerCount = None)
  }

  private case class State(newInnerState: InnerState, nextCommand: Stream[Throwable, Command])

  private sealed trait Command
  private object Command {
    final case class StopWithError(error: Throwable) extends Command
    object Stop                                      extends Command

    sealed trait Step extends Command {
      def run(oldInnerState: InnerState): State
    }

    final case class BrokersResponse(brokerIds: List[BrokerId]) extends Step {
      override def run(oldInnerState: InnerState): State = {
        val brokerCount = BrokerCount(brokerIds.length)
        val newState    = oldInnerState.copy(brokerCount = Some(brokerCount))

        State(
          newInnerState = newState,
          nextCommand = ZStream.fromZIO(kafkaService.getTopicSize(brokerIds).map(TopicSizeResponse)),
        )
      }
    }

    final case class TopicSizeResponse(topicsSize: Map[TopicName, TopicSize]) extends Step {
      override def run(oldInnerState: InnerState): State = {
        val topics = oldInnerState.topics.map { case (topicName, topicData) =>
          topicName -> topicData.copy(size = topicsSize.get(topicName))
        }
        State(
          newInnerState = oldInnerState.copy(topics = topics),
          nextCommand = ZStream.empty,
        )
      }
    }

    final case class TopicNamesResponse(topicNames: Seq[TopicName]) extends Step {
      override def run(oldInnerState: InnerState): State = {
        val topics = TreeMap.from(topicNames.map { topicName =>
          topicName -> TopicData(describeTopic = None, beginOffset = None, endOffset = None, size = None)
        })

        val ordersForFetchingTopics = ZStream
          .fromIterable(topics.keySet)
          .grouped(topicGroupSize)
          .mapZIO(kafkaService.describeTopics(_))
          .map(Command.TopicDescriptionResponse)

        State(
          newInnerState = oldInnerState.copy(topics = topics), //TODO: maybe do a smart merge if we go to pagination
          nextCommand = (if (!topics.isEmpty) ordersForFetchingTopics else ZStream(Command.Stop)),
        )
      }
    }

    final case class TopicPartitionsBeginOffsetResponse(offsets: Map[TopicPartition, Offset]) extends Step {
      override def run(oldInnerState: InnerState): State = {
        val newTopics = offsets.foldLeft(oldInnerState.topics) { case (topics, (TopicPartition(topicName, partition), offset)) =>
          topics.updatedWith(topicName)(_.map { topicData =>
            topicData.copy(beginOffset = Some(topicData.beginOffset.getOrElse(Map.empty).updated(partition, offset)))
          }.orElse(Some(TopicData(describeTopic = None, beginOffset = Some(Map(partition -> offset)), endOffset = None, size = None))))
        }

        State(
          newInnerState = oldInnerState.copy(topics = newTopics),
          nextCommand = ZStream.empty,
        )
      }
    }

    final case class TopicPartitionsEndOffsetResponse(offsets: Map[TopicPartition, Offset]) extends Step {
      override def run(oldInnerState: InnerState): State = {
        val newTopics = offsets.foldLeft(oldInnerState.topics) { case (topics, (TopicPartition(topicName, partition), offset)) =>
          topics.updatedWith(topicName)(_.map { topicData =>
            topicData.copy(endOffset = Some(topicData.endOffset.getOrElse(Map.empty).updated(partition, offset)))
          }.orElse(Some(TopicData(describeTopic = None, beginOffset = None, endOffset = Some(Map(partition -> offset)), size = None))))
        }

        State(
          newInnerState = oldInnerState.copy(topics = newTopics),
          nextCommand = ZStream.empty,
        )
      }
    }

    final case class TopicDescriptionResponse(topics: Map[TopicName, TopicDescription]) extends Step {

      def nextMessages = {
        val topicPartitions = ZStream
          .fromIterable(topics)
          .mapConcat { case (name, description) =>
            description.partition.keys
              .map(partition => Partition(partition.value))
              .map(partition => TopicPartition(name, partition))
          }
        topicPartitions
          .grouped(30)
          .flatMap(topicPartitions =>
            ZStream(
              kafkaService.beginningOffsets(topicPartitions).map(TopicPartitionsBeginOffsetResponse.apply),
              kafkaService.endOffsets(topicPartitions).map(TopicPartitionsEndOffsetResponse.apply),
            ).flattenZIO
          )
      }

      override def run(oldInnerState: InnerState): State = {
        val newTopics = topics.foldLeft(oldInnerState.topics) { case (topics, (topicName, topic)) =>
          topics.updatedWith(topicName)(_.map(_.copy(describeTopic = Some(topic))))
        }
        State(
          newInnerState = oldInnerState.copy(topics = newTopics),
          nextCommand = nextMessages,
        )
      }
    }

    object Init extends Step {
      override def run(oldInnerState: InnerState): State = State(
        newInnerState = oldInnerState,
        nextCommand = ZStream(
          kafkaService.getBrokerIds.map(Command.BrokersResponse(_)),
          kafkaService.listTopicNames.map(Command.TopicNamesResponse(_)),
        ).flattenZIO,
      )
    }

  }
}

object TopicInfoStreamServiceLive {
  val layer = ZLayer {
    for {
      kafkaService <- ZIO.service[KafkaService]
    } yield new TopicInfoStreamServiceLive(kafkaService)
  }

}
