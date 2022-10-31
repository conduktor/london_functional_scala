package io.conduktor.v2

import io.conduktor.KafkaService._
import io.conduktor.v2.Datapoint.DatapointMapOps
import io.conduktor.v2.LanguageExtensions.MapOps
import io.conduktor.v2.State.PartitionRecordCount
case class State(
  topicNames: Datapoint[Set[TopicName]],
  brokers: Datapoint[Set[BrokerId]],
  topicDescriptions: Map[TopicName, Datapoint[TopicDescription]],
  topicBeginOffsets: Map[TopicPartition, Datapoint[Offset]],
  topicEndOffsets: Map[TopicPartition, Datapoint[Offset]],
  topicSizes: Datapoint[Map[TopicName, TopicSize]],
  pageNum: Option[Int],
  pageSize: Int,
) {
  def isEmpty: Boolean =
    topicNames == Datapoint.Unknown &&
      brokers == Datapoint.Unknown

  val topicInPage: Set[TopicName] =
    pageNum
      .map { pageNum =>
        topicNames.whenLoaded { names =>
          names.toList.sorted
            .grouped(pageSize)
            .drop(pageNum - 1)
            .nextOption()
            .getOrElse(Nil)
        }
      }
      .getOrElse(Nil)
      .toSet

  def setPageNum(pageNum: Int): State = this.copy(pageNum = Some(pageNum))

  def listPartitionRecordCounts(topicNames: Set[TopicName]): Map[TopicPartition, PartitionRecordCount] = {
    val begins = topicBeginOffsets.filter { case (partition, _) => topicNames.contains(partition.topicName) }

    begins.join(topicEndOffsets) {
      case (Datapoint.Loaded(begin), Datapoint.Loaded(end)) => Some(PartitionRecordCount(end.value - begin.value))
      case _                                                => None
    }
  }

  def listTopicPartitions(topicName: TopicName): Set[TopicPartition] = topicDescriptions.get(topicName) match {
    case Some(Datapoint.Loaded(description)) => description.partition.keySet.map(partition => TopicPartition(topicName, partition))
    case _                                   => Set.empty
  }

  private def buildExpectedLoadingException(entityName: String, datapoint: Datapoint[?]) =
    new IllegalStateException(s"Expected $entityName to be Loading but it's ${datapoint.getClass.getName}")

  def updateTopicNames(topicNames: Set[TopicName]): (State, Diff) =
    this.topicNames match {
      case Datapoint.Unknown      => throw buildExpectedLoadingException("topicNames", Datapoint.Unknown)
      case l: Datapoint.Loaded[_] => throw buildExpectedLoadingException("topicNames", l)
      case Datapoint.Loading      =>
        copy(topicNames = Datapoint.Loaded(topicNames)) -> Diff.TopicNames(added = topicNames.toSeq)
    }

  def updateBrokers(brokers: Set[BrokerId]): (State, Diff) =
    this.brokers match {
      case Datapoint.Unknown      => throw buildExpectedLoadingException("brokers", Datapoint.Unknown)
      case l: Datapoint.Loaded[_] => throw buildExpectedLoadingException("brokers", l)
      case Datapoint.Loading      =>
        copy(brokers = Datapoint.Loaded(brokers)) -> Diff.Brokers(added = brokers.toSeq)
    }

  def updateSizes(sizes: Map[TopicName, TopicSize]): (State, Diff) =
    this.topicSizes match {
      case Datapoint.Unknown      => throw buildExpectedLoadingException("topicSizes", Datapoint.Unknown)
      case l: Datapoint.Loaded[_] => throw buildExpectedLoadingException("topicSizes", l)
      case Datapoint.Loading      =>
        copy(topicSizes = Datapoint.Loaded(sizes)) -> Diff.TopicSizes(added = sizes)
    }

  def updateBeginOffsets(offsets: Map[TopicPartition, Offset]): (State, Diff) = {
    val newOffsets = offsets.foldLeft(this.topicBeginOffsets) { case (offsets, (partition, offset)) =>
      offsets.updatedWith(partition) {
        case None                         => throw new IllegalArgumentException("Expected topicBeginOffsets to be present")
        case Some(Datapoint.Unknown)      => throw buildExpectedLoadingException("topicBeginOffsets", Datapoint.Unknown)
        case Some(l: Datapoint.Loaded[_]) => throw buildExpectedLoadingException("topicBeginOffsets", l)
        case Some(Datapoint.Loading)      => Some(Datapoint.Loaded(offset))
      }
    }
    copy(topicBeginOffsets = newOffsets) -> Diff.BeginOffset(added = offsets)
  }

  def updateEndOffsets(offsets: Map[TopicPartition, Offset]): (State, Diff) = {
    val newOffsets = offsets.foldLeft(this.topicEndOffsets) { case (offsets, (partition, offset)) =>
      offsets.updatedWith(partition) {
        case None                         => throw new IllegalArgumentException("Expected topicEndOffsets to be present")
        case Some(Datapoint.Unknown)      => throw buildExpectedLoadingException("topicEndOffsets", Datapoint.Unknown)
        case Some(l: Datapoint.Loaded[_]) => throw buildExpectedLoadingException("topicEndOffsets", l)
        case Some(Datapoint.Loading)      => Some(Datapoint.Loaded(offset))
      }
    }
    copy(topicEndOffsets = newOffsets) -> Diff.EndOffset(added = offsets)
  }

  def updateDescription(descriptions: Map[TopicName, TopicDescription]): (State, Diff) = {
    val newDescriptions = descriptions.foldLeft(this.topicDescriptions) { case (descriptions, (topicName, description)) =>
      descriptions.updatedWith(topicName) {
        case None                         => throw new IllegalArgumentException(s"Expected topicDescription for $topicName to be present")
        case Some(Datapoint.Unknown)      => throw buildExpectedLoadingException(s"topicDescription for $topicName", Datapoint.Unknown)
        case Some(l: Datapoint.Loaded[_]) => throw buildExpectedLoadingException(s"topicDescription for $topicName", l)
        case Some(Datapoint.Loading)      => Some(Datapoint.Loaded(description))
      }
    }
    //FIXME: should build topicpartitions and init offsets maps
    copy(topicDescriptions = newDescriptions) -> Diff.Description(added = descriptions)
  }

  def isComplete: Boolean = pageNum.isDefined &&
    topicDescriptions.view.filterKeys(topicInPage.contains).toMap.isComplete &&
    topicBeginOffsets.view.filterKeys(topicPartition => topicInPage.contains(topicPartition.topicName)).toMap.isComplete &&
    topicEndOffsets.view.filterKeys(topicPartition => topicInPage.contains(topicPartition.topicName)).toMap.isComplete &&
    topicNames.isComplete &&
    brokers.isComplete &&
    topicSizes.isComplete

}

object State {

  case class PartitionRecordCount(value: Long)

  def empty(pageSize: Int) = State(
    topicDescriptions = Map.empty,
    topicBeginOffsets = Map.empty,
    topicEndOffsets = Map.empty,
    topicNames = Datapoint.Unknown,
    brokers = Datapoint.Unknown,
    topicSizes = Datapoint.Unknown,
    pageNum = None,
    pageSize = pageSize,
  )

}
