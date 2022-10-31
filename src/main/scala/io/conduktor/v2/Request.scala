package io.conduktor.v2

import io.conduktor.KafkaService.{BrokerId, TopicName, TopicPartition}

sealed trait Request
object Request {
  case object FetchTopicNames                                        extends Request
  case object FetchBrokerIds                                         extends Request
  case class FetchTopicSize(brokerIds: Set[BrokerId])                extends Request
  case class FetchTopicDescription(topics: Set[TopicName])           extends Request
  case class FetchBeginOffsets(topicPartitions: Set[TopicPartition]) extends Request
  case class FetchEndOffsets(topicPartitions: Set[TopicPartition])   extends Request
}
