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
import zio.stream.{ZSink, ZStream, Stream}
import zio.{Queue, Task, ZIO, ZLayer}

import scala.collection.immutable.TreeMap

trait TopicInfoStreamService {

  def streamInfos: ZStream[Any, Throwable, Info]
}

object TopicInfoStreamService {

  implicit class MapOps[A: Ordering, B](self: TreeMap[A, B]) {
    def join[T](other: Map[A, T])(f: (B, T) => B): TreeMap[A, B] =
      TreeMap.from(
        self.map { case (key, value) =>
          other
            .get(key)
            .map(value -> _)
            .map(f.tupled)
            .map(key -> _)
            .getOrElse(key -> value)
        }
      )
  }

  sealed trait Info
  object Info {
    object Complete                                        extends Info
    case class Topics(topics: Set[TopicName])              extends Info
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

  override def streamInfos: Stream[Throwable, Info] = ZStream.unwrap(for {
    commands <- Queue.unbounded[Command].tap(_.offer(Command.Init))
  } yield streamInfosInternal(ZStream.fromQueue(commands)))

  def streamInfosInternal(commands: Stream[Throwable, Command]): Stream[Throwable, Info] = ZStream
    .fromZIO(for {
      responses <- Queue.unbounded[Command].tap(_.offer(Command.Init))
      stream     =
        ZStream
          .fromQueue(responses)
          .collectWhileZIO {
            case e: Command.Step          =>
              ZIO.succeed(e)
            case Command.StopWithError(t) =>
              ZIO.fail(t)
            //Order.Stop not handle so we stop the stream when it occur
          }
          .mapAccumZIO(State.empty) { (state, message: Command.Step) =>
            val nextCommands = computeNextCommand(state, message)
            val updatedState = computeNextState(state, message)
            ZStream
              .fromIterable(nextCommands)
              .flattenZIO
              .catchAll(e => ZStream.succeed(Command.StopWithError(e)))
              .run(ZSink.fromQueue(responses))
              .fork
              .as(updatedState -> computeInfos(state, updatedState))
          }
          .flatMap(infos => ZStream.fromIterable(infos))
    } yield stream)
    .flatten

  private def computeNextState(state: State, message: Command.Step): State =
    message match {
      case m: Command.BrokersResponse =>
        val brokerCount = BrokerCount(m.brokerIds.length)
        state.copy(brokerCount = Some(brokerCount))

      case m: Command.TopicSizeResponse =>
        state.updateTopics(forTopics = m.topicsSize.keys) { case (topicName, topicData) =>
          topicData.copy(size = m.topicsSize.get(topicName))
        }

      case m: Command.TopicNamesResponse =>
        state.updateTopics(forTopics = m.topicNames) { case (_, before) => before }

      case m: Command.TopicPartitionsBeginOffsetResponse =>
        val offsetsByTopic = m.offsets
          .groupMap { case (topicPartition, _) => topicPartition.topicName } { case (topicPartition, offset) =>
            (topicPartition.partition, offset)
          }
        val newTopics      = state.topics.join(offsetsByTopic) { case (data, offsets) => data.updateBeginOffsets(offsets.toMap) }
        state.setTopics(newTopics)

      case m: Command.TopicPartitionsEndOffsetResponse =>
        val offsetsByTopic = m.offsets
          .groupMap { case (topicPartition, _) => topicPartition.topicName } { case (topicPartition, offset) =>
            (topicPartition.partition, offset)
          }
        val newTopics      = state.topics.join(offsetsByTopic) { case (data, offsets) => data.updateEndOffsets(offsets.toMap) }
        state.setTopics(newTopics)

      case m: Command.TopicDescriptionResponse =>
        val updatedTopics = state.topics.join(m.topics) { case (data, description) =>
          data.copy(describeTopic = Some(description))
        }
        state.setTopics(updatedTopics)

      case Command.Init => state
    }

  //TODO: it let state in the argument list on purpose because we should take into account the
  //current state to know what the next command is because response ordering is non determinist
  private val topicGroupSize                                                               = 30
  private def computeNextCommand(state: State, message: Command.Step): List[Task[Command]] =
    message match {
      case Command.Init =>
        kafkaService.getBrokerIds.map(Command.BrokersResponse(_)) ::
          kafkaService.listTopicNames.map(names => Command.TopicNamesResponse(names.toSet)) :: Nil

      case m: Command.BrokersResponse =>
        kafkaService.getTopicSize(m.brokerIds).map(Command.TopicSizeResponse) :: Nil

      case m: Command.TopicNamesResponse =>
        m.topicNames
          .grouped(topicGroupSize)
          .map(topics => kafkaService.describeTopics(topics.toList).map(Command.TopicDescriptionResponse))
          .toList

      case m: Command.TopicDescriptionResponse =>
        val topicPartitions =
          m.topics.toList
            .flatMap { case (name, description) =>
              description.partition.keys
                .map(partition => Partition(partition.value))
                .map(partition => TopicPartition(name, partition))
            }
        topicPartitions
          .grouped(30)
          .toList
          .flatMap(topicPartitions =>
            List(
              kafkaService.beginningOffsets(topicPartitions).map(Command.TopicPartitionsBeginOffsetResponse),
              kafkaService.endOffsets(topicPartitions).map(Command.TopicPartitionsEndOffsetResponse.apply),
            )
          )

      case _: Command.TopicSizeResponse                  => Nil
      case _: Command.TopicPartitionsBeginOffsetResponse => Nil
      case _: Command.TopicPartitionsEndOffsetResponse   => Nil
    }

  private def computeInfos(oldState: State, newState: State): Iterable[Info] = {
    val newNames  = newState.topics.keySet -- oldState.topics.keySet
    val newTopics = if (newNames.nonEmpty) List(Info.Topics(newNames)) else List.empty

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

      size ++
        recordCountInfo ++
        partitionInfo ++
        replicationFactorInfo ++
        spreadInfo
    } ++ Option.when(newState.isComplete)(Info.Complete)
  }

  private final case class TopicData(
    describeTopic: Option[TopicDescription],
    beginOffset: Option[Map[Partition, Offset]],
    endOffset: Option[Map[Partition, Offset]],
    size: Option[TopicSize],
  ) {
    def updateBeginOffsets(offsets: Map[Partition, Offset]): TopicData = {
      val updatedOffsets = offsets.foldLeft(beginOffset.getOrElse(Map.empty)) { case (acc, (partition, offset)) =>
        acc.updated(partition, offset)
      }
      copy(beginOffset = Some(updatedOffsets))
    }

    def updateEndOffsets(offsets: Map[Partition, Offset]): TopicData = {
      val updatedOffsets = offsets.foldLeft(endOffset.getOrElse(Map.empty)) { case (acc, (partition, offset)) =>
        acc.updated(partition, offset)
      }
      copy(endOffset = Some(updatedOffsets))
    }

    val recordCount: Option[RecordCount]             =
      endOffset.zip(beginOffset).map { case (end, begin) =>
        val sumEndOfsets    = end.values.map(_.value).sum
        val sumStartOffsets = begin.values.map(_.value).sum
        RecordCount(sumEndOfsets - sumStartOffsets)
      }
    val partition: Option[Partition]                 = describeTopic.map(_.partition.size).map(Partition)
    val replicationFactor: Option[ReplicationFactor] = describeTopic.map(_.replicationFactor)

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
    val isComplete: Boolean                                                                              = maybeTopics.fold(ifEmpty = false)(_.forall { case (_, data) => data.isFull }) && brokerCount.isDefined
    val topics: TreeMap[TopicName, TopicData]                                                            = maybeTopics.getOrElse(TreeMap.empty)
    def setTopics(topics: TreeMap[TopicName, TopicData]): State                                          = copy(maybeTopics = Some(topics))
    def updateTopics(forTopics: Iterable[TopicName])(update: (TopicName, TopicData) => TopicData): State = setTopics(
      TreeMap.from[TopicName, TopicData]((topics.keySet ++ forTopics).toList.map { topicName =>
        topicName -> update(topicName, topics.getOrElse(topicName, TopicData.empty))
      })
    )
    def noTopics: State                                                                                  = copy(maybeTopics = None)
  }

  private object State {
    def empty: State = State(maybeTopics = None, brokerCount = None)
  }

  private sealed trait Command
  private object Command {
    final case class StopWithError(error: Throwable) extends Command
    object Stop                                      extends Command

    sealed trait Step                                                                   extends Command
    case class BrokersResponse(brokerIds: List[BrokerId])                               extends Step
    case class TopicSizeResponse(topicsSize: Map[TopicName, TopicSize])                 extends Step
    case class TopicNamesResponse(topicNames: Set[TopicName])                           extends Step
    case class TopicPartitionsBeginOffsetResponse(offsets: Map[TopicPartition, Offset]) extends Step
    case class TopicPartitionsEndOffsetResponse(offsets: Map[TopicPartition, Offset])   extends Step
    case class TopicDescriptionResponse(topics: Map[TopicName, TopicDescription])       extends Step
    case object Init                                                                    extends Step
  }
}

object TopicInfoStreamServiceLive {
  val layer = ZLayer {
    for {
      kafkaService <- ZIO.service[KafkaService]
    } yield new TopicInfoStreamServiceLive(kafkaService)
  }
}
