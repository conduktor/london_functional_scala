package io.conduktor.v2

import io.conduktor.KafkaService.{BrokerId, Offset, TopicDescription, TopicName, TopicPartition, TopicSize}

sealed trait Diff
object Diff {
  case class TopicNames(added: Seq[TopicName] = Seq.empty, removed: Seq[TopicName] = Seq.empty, updated: Seq[TopicName] = Seq.empty)
      extends Diff
  case class TopicSizes(
    added: Map[TopicName, TopicSize] = Map.empty,
    removed: Map[TopicName, TopicSize] = Map.empty,
    updated: Map[TopicName, TopicSize] = Map.empty,
  ) extends Diff
  case class Brokers(added: Seq[BrokerId] = Seq.empty, removed: Seq[BrokerId] = Seq.empty, updated: Seq[BrokerId] = Seq.empty) extends Diff
  case class BeginOffset(
    added: Map[TopicPartition, Offset] = Map.empty,
    removed: Map[TopicPartition, Offset] = Map.empty,
    updated: Map[TopicPartition, Offset] = Map.empty,
  ) extends Diff
  case class EndOffset(
    added: Map[TopicPartition, Offset] = Map.empty,
    removed: Map[TopicPartition, Offset] = Map.empty,
    updated: Map[TopicPartition, Offset] = Map.empty,
  ) extends Diff
  case class Description(
    added: Map[TopicName, TopicDescription] = Map.empty,
    removed: Map[TopicName, TopicDescription] = Map.empty,
    updated: Map[TopicName, TopicDescription] = Map.empty,
  ) extends Diff
  case class NewPage(pageNum: Int)                                                                                             extends Diff
  case object Empty                                                                                                            extends Diff
}
