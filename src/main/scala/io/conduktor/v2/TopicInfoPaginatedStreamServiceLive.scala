package io.conduktor.v2

import io.conduktor.KafkaService._
import io.conduktor.TopicInfoStreamService.Info
import io.conduktor.v2.Datapoint.DatapointMapOps
import io.conduktor.v2.State.PartitionRecordCount
import io.conduktor.KafkaService
import zio.stream.ZStream
import zio.{Queue, Task, ZIO, ZLayer}

trait TopicInfoPaginatedStreamService {

  def streamInfos(pageSize: Int, queue: Queue[Input.Command]): ZStream[Any, Throwable, Info]
}

class TopicInfoPaginatedStreamServiceLive(kafkaService: KafkaService) extends TopicInfoPaginatedStreamService {

  import TopicInfoPaginatedStreamServiceLive._

  override def streamInfos(pageSize: Int, queue: Queue[Input.Command]): Stream[Info] =
    ZStream
      .unwrap(for {
        responsesQueue       <- Queue.unbounded[Input.Response]
        inputs: Stream[Input] = ZStream.mergeAllUnbounded()(ZStream.fromQueue(queue), ZStream.fromQueue(responsesQueue))
      } yield {
        inputs
          .mapAccumZIO(State.empty(pageSize)) { case (state, input) =>
            val (stateUpdatedWithInput, diff) = applyInput(state, input)
            val requests                      = nextRequests(stateUpdatedWithInput, diff)
            val updatedState                  = requests.foldLeft(stateUpdatedWithInput)((s, request) => updateState(s, request))
            val infos                         = toInfo(updatedState, diff)
            executeRequests(responsesQueue)(requests).as((updatedState, infos))
          }
      })
      .flatMap(infos => ZStream.fromIterable(infos))

  def buildDescriptionInfo(state: State, topicNames: Set[TopicName]) =
    state.topicDescriptions
      .collect { case (topicName, Datapoint.Loaded(description)) if topicNames.contains(topicName) => topicName -> description }
      .flatMap { case (topicName, description) =>
        List(
          Info.ReplicationFactorInfo(topicName = topicName, replicationFactor = description.replicationFactor),
          Info.PartitionInfo(topicName = topicName, partition = Partition(description.partition.size)),
        ) ++ (state.brokers match {
          case Datapoint.Loaded(brokerIds) =>
            List(Info.SpreadInfo(topicName, kafkaService.topicSpread(BrokerCount(brokerIds.size), description)))
          case _                           => throw new RuntimeException("275")
        })
      }

  //TODO: deal with update/deleted
  def toInfo(state: State, diff: Diff): List[Info] =
    (diff match {
      case _: Diff.TopicNames =>
        val inPages = state.topicInPage
        Option.when(inPages.nonEmpty)(Info.Topics(inPages)).toList

      case instance: Diff.TopicSizes => instance.added.view.filterKeys(state.topicInPage.contains(_)).map(Info.Size.tupled.apply)

      case _: Diff.Brokers => Nil

      case instance: Diff.BeginOffset =>
        val topics = instance.added.keys.map(_.topicName).toSet
        buildRecordCountInfo(state, topics.filter(state.topicInPage.contains))

      case instance: Diff.EndOffset =>
        val topics = instance.added.keys.map(_.topicName).toSet
        buildRecordCountInfo(state, topics.filter(state.topicInPage.contains))

      case instance: Diff.Description =>
        buildDescriptionInfo(state, instance.added.keySet.filter(state.topicInPage.contains(_)))
      case Diff.Empty                 => Nil
      case Diff.NewPage(_)            =>
        //TODO: send all data already know about page
        Option.when(state.topicInPage.nonEmpty)(Info.Topics(state.topicInPage)).toList ++
          buildRecordCountInfo(state, state.topicInPage) ++
          buildDescriptionInfo(state, state.topicInPage) ++
          state.topicSizes.whenLoaded { topicSizes =>
            topicSizes.view.filterKeys(state.topicInPage.contains(_)).map(Info.Size.tupled.apply).toList
          }

    }).toList ++ Option.when(state.isComplete)(Info.Complete).toList

  def buildRecordCountInfo(state: State, topics: Set[TopicName]): List[Info.RecordCountInfo] = {
    val knowPartitionSizes: Map[TopicPartition, PartitionRecordCount] = state.listPartitionRecordCounts(topics)

    topics.toList.flatMap { topicName =>
      val topicPartitions = state.listTopicPartitions(topicName)
      Option.when(topicPartitions.forall(topicPartition => knowPartitionSizes.contains(topicPartition))) {
        val count = knowPartitionSizes.view.filterKeys(topicPartitions.contains).values.map(_.value).sum
        Info.RecordCountInfo(topicName, RecordCount(count))
      }
    }
  }

  def nextRequests(state: State, diff: Diff): List[Request] =
    if (state.isEmpty) List(Request.FetchTopicNames, Request.FetchBrokerIds) //TODO: reconsidere
    else
      diff match {
        case diff: Diff.Brokers =>
          state.topicNames.whenLoaded(_ => Option.when(diff.added.nonEmpty)(Request.FetchTopicSize(diff.added.toSet)).toList)

        case diff: Diff.TopicNames =>
          val hasTopicNames         = diff.added.nonEmpty
          val fetchTopicSize        = state.brokers.whenLoaded(brokerIds => Option.when(hasTopicNames)(Request.FetchTopicSize(brokerIds)).toList)
          val fetchTopicDescription = nextTopicDescriptionsToFetch(state)

          fetchTopicSize ++ fetchTopicDescription

        case diff: Diff.Description =>
          val hasDescriptions = diff.added.nonEmpty
          if (hasDescriptions) {
            nextTopicDescriptionsToFetch(state) ++ fetchBeginPartitionOffsets(state) ++ fetchEndPartitionOffsets(state)
          } else {
            Nil
          }
        case Diff.NewPage(_)        =>
          nextTopicDescriptionsToFetch(state)

        case _: Diff.BeginOffset => fetchBeginPartitionOffsets(state)
        case _: Diff.EndOffset   => fetchEndPartitionOffsets(state)
        case _: Diff.TopicSizes  => Nil
        case Diff.Empty          => Nil
      }

  def nextTopicDescriptionsToFetch(state: State): List[Request] =
    state.topicNames.whenLoaded { _ =>
      val topicInPages = state.topicInPage
      val nextTopics   = state.topicDescriptions.view
        .filterKeys(topicInPages.contains)
        .toMap
        .nextUnknownChunk(defaultChunkSize)
        .toSet

      Option.when(nextTopics.nonEmpty)(Request.FetchTopicDescription(nextTopics)).toList
    }

  def fetchBeginPartitionOffsets(state: State): List[Request] =
    state.topicNames.whenLoaded { _ =>
      val topicInPages     = state.topicInPage
      val nextBeginOffsets = state.topicBeginOffsets
        .filter { case (topicPartition: TopicPartition, _) => topicInPages.contains(topicPartition.topicName) }
        .nextUnknownChunk(defaultChunkSize)
        .toSet

      Option.when(nextBeginOffsets.nonEmpty)(Request.FetchBeginOffsets(nextBeginOffsets)).toList
    }

  def fetchEndPartitionOffsets(state: State): List[Request] =
    state.topicNames.whenLoaded { _ =>
      val topicInPages   = state.topicInPage
      val nextEndOffsets = state.topicEndOffsets
        .filter { case (topicPartition: TopicPartition, _) => topicInPages.contains(topicPartition.topicName) }
        .nextUnknownChunk(defaultChunkSize)
        .toSet
      Option.when(nextEndOffsets.nonEmpty)(Request.FetchEndOffsets(nextEndOffsets)).toList
    }

  def executeRequests(queue: Queue[Input.Response])(requests: List[Request]): Task[Unit] =
    ZStream.fromIterable(requests).mapZIO(executeRequest).foreach(queue.offer)

  def executeRequest(request: Request): Task[Input.Response] = request match {
    case Request.FetchTopicNames =>
      kafkaService.listTopicNames.map(names => Input.Response.TopicNames(names.toSet))

    case Request.FetchBrokerIds =>
      kafkaService.getBrokerIds.map(brokerIds => Input.Response.Brokers(brokerIds.toSet))

    case fetch: Request.FetchTopicSize =>
      kafkaService.getTopicSize(fetch.brokerIds.toList).map(Input.Response.TopicSizes)

    case fetch: Request.FetchTopicDescription =>
      kafkaService.describeTopics(fetch.topics.toSeq).map(Input.Response.TopicDescriptions)

    case fetch: Request.FetchBeginOffsets =>
      kafkaService.beginningOffsets(fetch.topicPartitions.toSeq).map(Input.Response.TopicPartitionsBeginOffset)

    case fetch: Request.FetchEndOffsets =>
      kafkaService.endOffsets(fetch.topicPartitions.toSeq).map(Input.Response.TopicPartitionsEndOffset)

  }

  def applyInput(state: State, input: Input): (State, Diff) =
    input match {
      case Input.Command.Subscribe(pageNum)                    => (state.setPageNum(pageNum), Diff.NewPage(pageNum)) //TODO: handle page
      case response: Input.Response.Brokers                    => state.updateBrokers(response.brokerIds)
      case response: Input.Response.TopicNames                 =>
        val (stateWithNames, diffForNames) = state.updateTopicNames(response.topicNames)
        val stateWithDescriptions          = stateWithNames.copy(topicDescriptions = response.topicNames.map(_ -> Datapoint.Unknown).toMap)

        (stateWithDescriptions, diffForNames)
      case response: Input.Response.TopicSizes                 => state.updateSizes(response.topicsSize)
      case response: Input.Response.TopicPartitionsBeginOffset => state.updateBeginOffsets(response.offsets)
      case response: Input.Response.TopicPartitionsEndOffset   => state.updateEndOffsets(response.offsets)
      case response: Input.Response.TopicDescriptions          =>
        val (stateWithDescriptions, diffForDescription) = state.updateDescription(response.topics)

        val newPartitions: Map[TopicPartition, Datapoint[Offset]] =
          response.topics.flatMap { case (topicName, topicDescription: TopicDescription) =>
            topicDescription.partition.map { case (partition, _) => TopicPartition(topicName, partition) -> Datapoint.Unknown }
          }

        val newState = stateWithDescriptions.copy(
          topicBeginOffsets = newPartitions ++ stateWithDescriptions.topicBeginOffsets,
          topicEndOffsets = newPartitions ++ stateWithDescriptions.topicEndOffsets,
        )

        (newState, diffForDescription)
    }

}

object TopicInfoPaginatedStreamServiceLive {
  val defaultChunkSize = 30

  def updateState(state: State, request: Request): State = request match {
    case fetch: Request.FetchTopicDescription =>
      state.copy(topicDescriptions = fetch.topics.foldLeft(state.topicDescriptions) { case (descriptions, topicName) =>
        descriptions.updated(topicName, Datapoint.Loading)
      })

    case fetch: Request.FetchEndOffsets =>
      state.copy(topicEndOffsets = fetch.topicPartitions.foldLeft(state.topicEndOffsets) { case (offsets, topicPartition) =>
        offsets.updated(topicPartition, Datapoint.Loading)
      })

    case fetch: Request.FetchBeginOffsets =>
      state.copy(topicBeginOffsets = fetch.topicPartitions.foldLeft(state.topicBeginOffsets) { case (offsets, topicPartition) =>
        offsets.updated(topicPartition, Datapoint.Loading)
      })

    case _: Request.FetchTopicSize => state.copy(topicSizes = Datapoint.Loading)
    case Request.FetchTopicNames   => state.copy(topicNames = Datapoint.Loading)
    case Request.FetchBrokerIds    => state.copy(brokers = Datapoint.Loading)
  }

  type Stream[A] = ZStream[Any, Throwable, A]

  val layer: ZLayer[KafkaService, Nothing, TopicInfoPaginatedStreamServiceLive] = ZLayer {
    for {
      kafkaService <- ZIO.service[KafkaService]
    } yield new TopicInfoPaginatedStreamServiceLive(kafkaService)
  }
}
