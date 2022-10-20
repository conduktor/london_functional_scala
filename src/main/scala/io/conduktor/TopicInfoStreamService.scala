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
      queue <- Queue.unbounded[Message]
      _     <- queue.offer(Message.Init)
      stream =
        ZStream
          .fromQueue(queue)
          .collectWhileZIO {
            case e: KafkaMessage        =>
              ZIO.succeed(e)
            case Message.StopWithError(t) =>
              ZIO.fail(t)
            //Order.Stop not handle so we stop the stream when it occur
          }
          .mapAccumZIO(State.empty) { (state, message: KafkaMessage) =>
            val orderResult = message.run(oldState = state)
            orderResult.nextMessages
              .catchAll { e =>
                ZStream.succeed(Message.StopWithError(e))
              }
              .run(ZSink.fromQueue(queue))
              .fork
              .as(orderResult.newState -> orderResult.output)
          }
          .flatten
    } yield stream)
    .flatten

  val topicGroupSize = 30
  private final case class TopicData(
    describeTopic: Option[TopicDescription],
    beginOffset: Option[Offset],
    endOffset: Option[Offset],
    size: Option[TopicSize],
  )

  private case class State(topics: TreeMap[TopicName, TopicData], brokerCount: Option[BrokerCount])

  private object State {
    def empty: State = State(topics = TreeMap.empty, brokerCount = None)
  }

  private case class MessageResult(newState: State, output: UStream[Info], nextMessages: Stream[Throwable, Message])

  private sealed trait Message
  private sealed trait KafkaMessage extends Message {
    def run(oldState: State): MessageResult
  }
  private object Message {
    final case class BrokersResponse(brokerIds: List[BrokerId]) extends KafkaMessage {
      override def run(oldState: State): MessageResult = {
        val brokerCount = BrokerCount(brokerIds.length)
        val newState    = oldState.copy(brokerCount = Some(brokerCount))

        MessageResult(
          newState = newState,
          output = ZStream.fromIterable(newState.topics).mapConcat { case (topicName, topicData) =>
            topicData.describeTopic.toList.map { describe =>
              Info.SpreadInfo(topicName, kafkaService.topicSpread(brokerCount, describe))
            }
          },
          nextMessages = ZStream.fromZIO(kafkaService.getTopicSize(brokerIds).map(TopicSizeResponse)),
        )
      }
    }

    final case class TopicSizeResponse(topicsSize: Map[TopicName, TopicSize]) extends KafkaMessage {
      override def run(oldState: State): MessageResult = {
        val topics = oldState.topics.map { case (topicName, topicData) =>
          topicName -> topicData.copy(size = topicsSize.get(topicName))
        }
        MessageResult(
          newState = oldState.copy(topics = topics),
          output = ZStream.fromIterable(topics).mapConcat { case (topicName, topicData) =>
            topicData.size.map { size =>
              Info.Size(topicName = topicName, size = size)
            }
          },
          nextMessages = ZStream.empty,
        )
      }
    }

    final case class TopicNamesResponse(topicNames: Seq[TopicName]) extends KafkaMessage {
      override def run(oldState: State): MessageResult = {
        val topics = TreeMap.from(topicNames.map { topicName =>
          topicName -> TopicData(describeTopic = None, beginOffset = None, endOffset = None, size = None)
        })

        val ordersForFetchingTopics = ZStream
          .fromIterable(topics.keySet)
          .grouped(topicGroupSize)
          .mapZIO(kafkaService.describeTopics(_))
          .map(Message.TopicDescriptionResponse)

        val stopOrder = ZStream.succeed(if (topics.isEmpty) Message.Stop else Message.Stop) //start begin/end offset

        MessageResult(
          newState = oldState.copy(topics = topics), //TODO: maybe do a smart merge if we go to pagination
          output = ZStream.succeed(Info.Topics(topicNames)),
          nextMessages = ordersForFetchingTopics ++ stopOrder,
        )
      }
    }

    final case class TopicPartitionsBeginOffsetResponse(offsets: Map[TopicPartition, Offset]) extends KafkaMessage {
      override def run(oldState: State): MessageResult = {
        val newTopics = offsets.foldLeft(oldState.topics){case (topics, (partition, offset)) =>
          topics.get(partition.topicName)
        }
      }
    }

    final case class TopicPartitionsEndOffsetResponse(offsets: Map[TopicPartition, Offset]) extends KafkaMessage {
      override def run(oldState: State): MessageResult = ???
    }

    final case class TopicDescriptionResponse(topics: Map[TopicName, TopicDescription]) extends KafkaMessage {

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
              kafkaService.endOffsets(topicPartitions).map(TopicPartitionsEndOffsetResponse.apply))
              .flattenZIO
          )
      }

      override def run(oldState: State): MessageResult = {
        val newTopics = topics.foldLeft(oldState.topics) { case (topics, (topicName, topic)) =>
          topics.updatedWith(topicName)(_.map(_.copy(describeTopic = Some(topic))))
        }
        MessageResult(
          newState = oldState.copy(topics = newTopics),
          output = ZStream.fromIterable(newTopics.view.filterKeys(topics.contains)).mapConcat { case (topicName, stateEntry: TopicData) =>
            val topicDescription = stateEntry.describeTopic.get //TODO: be cleaner
            List(
              Info.PartitionInfo(topicName, Partition(topicDescription.partition.size)),
              Info.ReplicationFactorInfo(topicName, topicDescription.replicationFactor),
            ) ++ oldState.brokerCount
              .map { brokerCount =>
                List(Info.SpreadInfo(topicName, kafkaService.topicSpread(brokerCount, topicDescription)))
              }
              .getOrElse(List.empty)
          },
          nextMessages = nextMessages,
        )
      }
    }

    object Init extends KafkaMessage {
      override def run(oldState: State): MessageResult = MessageResult(
        newState = oldState,
        output = ZStream.empty,
        nextMessages = ZStream.fromIterableZIO(
          ZIO.foreach(
            List(
              kafkaService.listTopicNames.map(Message.TopicNamesResponse(_)),
              kafkaService.getBrokerIds.map(Message.BrokersResponse(_)),
            )
          )(identity)
        ),
      )
    }

    final case class StopWithError(error: Throwable) extends Message

    object Stop extends Message
  }
}

object TopicInfoStreamServiceLive {
  val layer = ZLayer {
    for {
      kafkaService <- ZIO.service[KafkaService]
    } yield new TopicInfoStreamServiceLive(kafkaService)
  }

}
