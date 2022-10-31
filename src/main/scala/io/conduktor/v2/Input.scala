package io.conduktor.v2

import io.conduktor.KafkaService._

sealed trait Input

object Input {
  sealed trait Response extends Input

  object Response {
    case class Brokers(brokerIds: Set[BrokerId]) extends Response

    case class TopicSizes(topicsSize: Map[TopicName, TopicSize]) extends Response

    case class TopicNames(topicNames: Set[TopicName]) extends Response

    case class TopicPartitionsBeginOffset(offsets: Map[TopicPartition, Offset]) extends Response

    case class TopicPartitionsEndOffset(offsets: Map[TopicPartition, Offset]) extends Response

    case class TopicDescriptions(topics: Map[TopicName, TopicDescription]) extends Response
  }

  sealed trait Command extends Input

  object Command {
    case class Subscribe(page: Int) extends Command
  }
}
