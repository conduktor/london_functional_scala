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
  TopicSize,
}
import io.conduktor.TopicInfoStreamService.Info
import zio.{Queue, ZIO, ZLayer}
import zio.stream.{Stream, UStream, ZSink, ZStream}

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
      queue <- Queue.unbounded[Order]
      _     <- queue.offer(Order.Init)
      stream =
        ZStream
          .fromQueue(queue)
          .collectWhileZIO {
            case e: KafkaOrder          =>
              ZIO.succeed(e)
            case Order.StopWithError(t) =>
              ZIO.fail(t)
            //Order.Stop not handle so we stop the stream when it occur
          }
          .mapAccumZIO(State.empty) { (state, order: KafkaOrder) =>
            val orderResult = order.run(oldState = state)
            orderResult.nextOrders
              .catchAll { e =>
                ZStream.succeed(Order.StopWithError(e))
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

  private case class OrderResult(newState: State, output: UStream[Info], nextOrders: Stream[Throwable, Order])

  private sealed trait Order
  private sealed trait KafkaOrder extends Order {
    def run(oldState: State): OrderResult
  }
  private object Order {
    final case class BrokersResponse(brokerIds: List[BrokerId]) extends KafkaOrder {
      override def run(oldState: State): OrderResult = {
        val brokerCount = BrokerCount(brokerIds.length)
        val newState    = oldState.copy(brokerCount = Some(brokerCount))

        OrderResult(
          newState = newState,
          output = ZStream.fromIterable(newState.topics).mapConcat { case (topicName, topicData) =>
            topicData.describeTopic.toList.map { describe =>
              Info.SpreadInfo(topicName, kafkaService.topicSpread(brokerCount, describe))
            }
          },
          nextOrders = ZStream.fromZIO(kafkaService.getTopicSize(brokerIds).map(TopicSizeResponse)),
        )
      }
    }

    final case class TopicSizeResponse(topicsSize: Map[TopicName, TopicSize]) extends KafkaOrder {
      override def run(oldState: State): OrderResult = {
        val topics = oldState.topics.map { case (topicName, topicData) =>
          topicName -> topicData.copy(size = topicsSize.get(topicName))
        }
        OrderResult(
          newState = oldState.copy(topics = topics),
          output = ZStream.fromIterable(topics).mapConcat { case (topicName, topicData) =>
            topicData.size.map { size =>
              Info.Size(topicName = topicName, size = size)
            }
          },
          nextOrders = ZStream.empty,
        )
      }
    }

    final case class TopicNamesResponse(topicNames: Seq[TopicName]) extends KafkaOrder {
      override def run(oldState: State): OrderResult = {
        val topics = TreeMap.from(topicNames.map { topicName =>
          topicName -> TopicData(describeTopic = None, beginOffset = None, endOffset = None, size = None)
        })

        val ordersForFetchingTopics = ZStream
          .fromIterable(topics.keySet)
          .grouped(topicGroupSize)
          .mapZIO(kafkaService.describeTopics(_))
          .map(Order.TopicResponse)

        val stopOrder = ZStream.succeed(if (topics.isEmpty) Order.Stop else Order.Stop) //start begin/end offset

        OrderResult(
          newState = oldState.copy(topics = topics), //TODO: maybe do a smart merge if we go to pagination
          output = ZStream.succeed(Info.Topics(topicNames)),
          nextOrders = ordersForFetchingTopics ++ stopOrder,
        )
      }
    }

    final case class TopicResponse(topics: Map[TopicName, TopicDescription]) extends KafkaOrder {
      override def run(oldState: State): OrderResult = {
        val newTopics = topics.foldLeft(oldState.topics) { case (topics, (topicName, topic)) =>
          topics.updatedWith(topicName)(_.map(_.copy(describeTopic = Some(topic))))
        }
        OrderResult(
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
          nextOrders = ZStream.empty, //TODO: end / start offset
        )
      }
    }

    object Init extends KafkaOrder {
      override def run(oldState: State): OrderResult = OrderResult(
        newState = oldState,
        output = ZStream.empty,
        nextOrders = ZStream.fromIterableZIO(
          ZIO.foreach(
            List(
              kafkaService.listTopicNames.map(Order.TopicNamesResponse(_)),
              kafkaService.getBrokerIds.map(Order.BrokersResponse(_)),
            )
          )(identity)
        ),
      )
    }

    final case class StopWithError(error: Throwable) extends Order

    object Stop extends Order
  }
}

object TopicInfoStreamServiceLive {
  val layer = ZLayer {
    for {
      kafkaService <- ZIO.service[KafkaService]
    } yield new TopicInfoStreamServiceLive(kafkaService)
  }

}
