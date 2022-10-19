package io.conduktor

import io.circe.generic.semiauto.deriveCodec
import io.circe.{Codec, Decoder, Encoder, KeyDecoder, KeyEncoder}
import io.conduktor.KafkaService.{
  BrokerId,
  Offset,
  Partition,
  PartitionCount,
  PartitionInfo,
  RecordCount,
  ReplicationFactor,
  Spread,
  TopicDescription,
  TopicName,
  TopicPartition,
  TopicSize,
}

object CirceCodec {
  implicit val partition: Codec[Partition] = Codec
    .from(Decoder.decodeInt, Encoder.encodeInt)
    .iemap(anInt => Right(Partition(anInt)))(_.value)

  implicit val topicSize: Codec[TopicSize] = Codec
    .from(Decoder.decodeLong, Encoder.encodeLong)
    .iemap(aLong => Right(TopicSize(aLong)))(_.value)

  implicit val offset: Codec[Offset] = Codec
    .from(Decoder.decodeLong, Encoder.encodeLong)
    .iemap(aLong => Right(Offset(aLong)))(_.value)

  implicit val brokerIdCodec: Codec[BrokerId] = Codec
    .from(Decoder.decodeInt, Encoder.encodeInt)
    .iemap(anInt => Right(BrokerId(anInt)))(_.value)

  implicit val recordCountCodec: Codec[RecordCount] = Codec
    .from(Decoder.decodeLong, Encoder.encodeLong)
    .iemap(anLong => Right(RecordCount(anLong)))(_.value)

  implicit val topicSpread: Codec[Spread] = Codec
    .from(Decoder.decodeDouble, Encoder.encodeDouble)
    .iemap(aDouble => Right(Spread(aDouble)))(_.value)

  implicit val topicNameCodec: Codec[TopicName] =
    Codec
      .from(Decoder.decodeString, Encoder.encodeString)
      .iemap(str => Right(TopicName(str)))(_.value)

  implicit val topicPartition: Codec[TopicPartition] =
    deriveCodec[TopicPartition]

  implicit val partitionInfo: Codec[PartitionInfo] =
    deriveCodec[PartitionInfo]

  implicit val partitionKeyEncoder: KeyEncoder[Partition] =
    KeyEncoder.encodeKeyInt.contramap[Partition](_.value)

  implicit val partitionKeyDecoder: KeyDecoder[Partition] =
    KeyDecoder.decodeKeyInt.map[Partition](Partition)

  implicit val topicNameKeyEncoder: KeyEncoder[TopicName] =
    KeyEncoder.encodeKeyString.contramap[TopicName](_.value)

  implicit val topicNameKeyDecoder: KeyDecoder[TopicName] =
    KeyDecoder.decodeKeyString.map[TopicName](TopicName(_))

  implicit val topicDescription: Codec[TopicDescription] =
    deriveCodec[TopicDescription]

  implicit val replicationFactorCodec: Codec[ReplicationFactor] = Codec
    .from(Decoder.decodeInt, Encoder.encodeInt)
    .iemap(anInt => Right(ReplicationFactor(anInt)))(_.value)

  implicit val partitionCountCodec: Codec[PartitionCount] = Codec
    .from(Decoder.decodeInt, Encoder.encodeInt)
    .iemap(anInt => Right(PartitionCount(anInt)))(_.value)
}
