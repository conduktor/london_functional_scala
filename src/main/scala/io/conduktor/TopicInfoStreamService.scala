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
    object Complete                                        extends Info
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
            val orderResult = message.run(oldState = state)
            orderResult.nextCommand
              .catchAll(e => ZStream.succeed(Command.StopWithError(e)))
              .run(ZSink.fromQueue(queue))
              .fork
              .as(orderResult.newState -> computeOutput(state, orderResult.newState))
          }
          .flatten
    } yield stream)
    .flatten

  private def computeOutput(oldState: State, newState: State): UStream[Info] =
    ZStream.fromIterable {
      val newNames  = newState.topics.keySet -- oldState.topics.keySet
      val newTopics = if (newNames.nonEmpty) List(Info.Topics(newNames.toList)) else List.empty
      newTopics ++ newState.topics.flatMap { case (topicName, newTopicData) =>
        val oldTopicData = oldState.topics.getOrElse(topicName, TopicData.empty)
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

        val complete = Option.when(newState.isComplete)(Info.Complete)

        List(
          size,
          recordCountInfo,
          partitionInfo,
          replicationFactorInfo,
          spreadInfo,
          complete,
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
    val recordCount: Option[RecordCount]                         =
      endOffset.zip(beginOffset).map { case (end, begin) =>
        val sumEndOfsets    = end.values.map(_.value).sum
        val sumStartOffsets = begin.values.map(_.value).sum
        RecordCount(sumEndOfsets - sumStartOffsets)
      }
    val partition: Option[Partition]                             = describeTopic.map(_.partition.size).map(Partition)
    val replicationFactor: Option[ReplicationFactor]             = describeTopic.map(_.replicationFactor)
    def spread(brokerCount: Option[BrokerCount]): Option[Spread] =
      describeTopic.zip(brokerCount).map { case (describe, brokerCount) => kafkaService.topicSpread(brokerCount, describe) }

    val isFull =
      describeTopic.isDefined &&
        beginOffset.isDefined &&
        endOffset.isDefined &&
        size.isDefined
  }
  private object TopicData {
    def empty: TopicData = TopicData(describeTopic = None, beginOffset = None, endOffset = None, size = None)
  }

  private case class State(maybeTopics: Option[TreeMap[TopicName, TopicData]], brokerCount: Option[BrokerCount]) {
    val isComplete: Boolean                                     = maybeTopics.fold(ifEmpty = false)(_.forall { case (_, data) => data.isFull }) && brokerCount.isDefined
    val topics: TreeMap[TopicName, TopicData]                       = maybeTopics.getOrElse(TreeMap.empty)
    def setTopics(topics: TreeMap[TopicName, TopicData]): State = copy(maybeTopics = Some(topics))
  }

  private object State {
    def empty: State = State(maybeTopics = None, brokerCount = None)
  }

  private class StepResult(val newState: State, val nextCommand: Stream[Throwable, Command])

  private sealed trait Command
  private object Command {
    final case class StopWithError(error: Throwable) extends Command
    object Stop                                      extends Command

    sealed trait Step extends Command {
      def run(oldState: State): StepResult
    }

    final case class BrokersResponse(brokerIds: List[BrokerId]) extends Step {
      override def run(oldState: State): StepResult = {
        val brokerCount = BrokerCount(brokerIds.length)
        val newState    = oldState.copy(brokerCount = Some(brokerCount))

        new StepResult(
          newState = newState,
          nextCommand = ZStream.fromZIO(kafkaService.getTopicSize(brokerIds).map(TopicSizeResponse)),
        )
      }
    }

    final case class TopicSizeResponse(topicsSize: Map[TopicName, TopicSize]) extends Step {
      override def run(oldState: State): StepResult = {
        val topics = oldState.topics.map { case (topicName, topicData) =>
          topicName -> topicData.copy(size = topicsSize.get(topicName))
        }
        new StepResult(
          newState = oldState.setTopics(topics),
          nextCommand = ZStream.empty,
        )
      }
    }

    final case class TopicNamesResponse(topicNames: Seq[TopicName]) extends Step {
      override def run(oldState: State): StepResult = {
        val topics = TreeMap.from(topicNames.map { topicName =>
          topicName -> TopicData(describeTopic = None, beginOffset = None, endOffset = None, size = None)
        })

        val ordersForFetchingTopics = ZStream
          .fromIterable(topics.keySet)
          .grouped(topicGroupSize)
          .mapZIO(kafkaService.describeTopics(_))
          .map(Command.TopicDescriptionResponse)

        new StepResult(
          newState = oldState.setTopics(topics = topics), //TODO: maybe do a smart merge if we go to pagination
          nextCommand = ordersForFetchingTopics,
        )
      }
    }

    final case class TopicPartitionsBeginOffsetResponse(offsets: Map[TopicPartition, Offset]) extends Step {
      override def run(oldState: State): StepResult = {
        val newTopics = offsets.foldLeft(oldState.topics) { case (topics, (TopicPartition(topicName, partition), offset)) =>
          topics.updatedWith(topicName)(_.map { topicData =>
            topicData.copy(beginOffset = Some(topicData.beginOffset.getOrElse(Map.empty).updated(partition, offset)))
          }.orElse(Some(TopicData(describeTopic = None, beginOffset = Some(Map(partition -> offset)), endOffset = None, size = None))))
        }

        new StepResult(
          newState = oldState.setTopics(newTopics),
          nextCommand = ZStream.empty,
        )
      }
    }

    final case class TopicPartitionsEndOffsetResponse(offsets: Map[TopicPartition, Offset]) extends Step {
      override def run(oldState: State): StepResult = {
        val newTopics = offsets.foldLeft(oldState.topics) { case (topics, (TopicPartition(topicName, partition), offset)) =>
          topics.updatedWith(topicName)(_.map { topicData =>
            topicData.copy(endOffset = Some(topicData.endOffset.getOrElse(Map.empty).updated(partition, offset)))
          }.orElse(Some(TopicData(describeTopic = None, beginOffset = None, endOffset = Some(Map(partition -> offset)), size = None))))
        }

        new StepResult(
          newState = oldState.setTopics(newTopics),
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

      override def run(oldState: State): StepResult = {
        val newTopics = topics.foldLeft(oldState.topics) { case (topics, (topicName, topic)) =>
          topics.updatedWith(topicName)(_.map(_.copy(describeTopic = Some(topic))))
        }
        new StepResult(
          newState = oldState.setTopics(newTopics),
          nextCommand = nextMessages,
        )
      }
    }

    object Init extends Step {
      override def run(oldState: State): StepResult = new StepResult(
        newState = oldState,
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
