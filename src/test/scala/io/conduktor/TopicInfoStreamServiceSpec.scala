package io.conduktor

import io.conduktor.KafkaService._
import io.conduktor.TopicInfoStreamService.Info
import zio._
import zio.kafka.admin.AdminClient
import zio.kafka.admin.AdminClient.NewTopic
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test.TestAspect.{samples, sequential, shrinks, timeout}
import zio.test._

object TopicInfoStreamServiceSpec extends ZIOSpecDefault {

  val runTopicInfoStream = ZStream
    .serviceWithStream[TopicInfoStreamService](
      _.streamInfos
    )
    .takeWhile(!_.isInstanceOf[Info.Complete.type])
    .runCollect

  case class Record(key: String, value: String)

  val anyTopic: Gen[Any, NewTopic] =
    (Gen.alphaNumericString.filter(!_.isBlank).map(TopicName(_)) <*> Gen.small(size => Gen.const(TopicSize(size))))
      .map { case (name, size) =>
        NewTopic(name.value, size.value.toInt + 1, replicationFactor = 1)
      }

  val anyRecord: Gen[Any, Record] =
    (Gen.alphaNumericString <*> Gen.alphaNumericString).map { case (key, value) =>
      Record(key, value)
    }

  val shouldHandleNoTopicSpec = test("should terminate even when no topics") {
    assertZIO(runTopicInfoStream)(isEmpty)
  }

  val returnTopicsNamesSpec =
    test("should first return some topic names") {
      val topicOne   = TopicName("topicOne")
      val topicTwo   = TopicName("topicTwo")
      val topicThree = TopicName("topicThree")
      val topicFour  = TopicName("topicFour")
      for {
        _     <- KafkaUtils.createTopic(name = topicOne)
        _     <- KafkaUtils.createTopic(name = topicTwo)
        _     <- KafkaUtils.createTopic(name = topicThree)
        _     <- KafkaUtils.createTopic(name = topicFour)
        names <- ZStream
                   .serviceWithStream[TopicInfoStreamService](
                     _.streamInfos
                   )
                   .runHead
      } yield assert(names)(
        isSome(
          isSubtype[Info.Topics](
            hasField(
              "names",
              _.topics,
              hasSameElements(Seq(topicOne, topicTwo, topicThree, topicFour)),
            )
          )
        )
      )
    }

  val sizeExampleSpec = test("should return size of topics - example") {
    val topicOne   = TopicName("topicOne")
    val topicTwo   = TopicName("topicTwo")
    val topicThree = TopicName("topicThree")
    val topicFour  = TopicName("topicFour")
    for {
      _     <- KafkaUtils.createTopic(name = topicOne)
      _     <- KafkaUtils.produce(topic = topicOne, key = "bar", value = "foo1")
      _     <- KafkaUtils.produce(topic = topicOne, key = "bar", value = "foo2")
      _     <- KafkaUtils.createTopic(name = topicTwo)
      _     <- KafkaUtils.produce(topic = topicTwo, key = "bar", value = "foo1")
      _     <- KafkaUtils.produce(topic = topicTwo, key = "bar", value = "foo2")
      _     <- KafkaUtils.produce(topic = topicTwo, key = "bar", value = "foo3")
      _     <- KafkaUtils.createTopic(name = topicThree)
      _     <- KafkaUtils.produce(topic = topicThree, key = "bar", value = "foo1")
      _     <- KafkaUtils.produce(topic = topicThree, key = "bar", value = "foo2")
      _     <- KafkaUtils.produce(topic = topicThree, key = "bar", value = "foo3")
      _     <- KafkaUtils.produce(topic = topicThree, key = "bar", value = "foo4")
      _     <- KafkaUtils.createTopic(name = topicFour)
      infos <- runTopicInfoStream
    } yield assert(infos)(
      hasSubset(
        Seq(
          Info.Size(topicOne, TopicSize(150)),
          Info.Size(topicTwo, TopicSize(225)),
          Info.Size(topicThree, TopicSize(300)),
          Info.Size(topicFour, TopicSize(0)),
        )
      )
    )
  }

  val sizePropertyTestingSpec = test("should return size of topics") {
    check(Gen.mapOf(anyTopic, Gen.setOf(anyRecord))) { topicsWithRecords =>
      for {
        expected <- ZIO
                      .foreach(topicsWithRecords.toList) { case (topic, records) =>
                        for {
                          topicName <- ZIO.serviceWithZIO[MakeTopicNameUnique](_.makeTopicNameUnique(topic.name))
                          _         <- KafkaUtils.createTopic(topicName)
                          size      <- ZIO.foldLeft(records)(0) { case (acc, record) =>
                                         KafkaUtils
                                           .produce(
                                             topic = topicName,
                                             key = record.key,
                                             value = record.value,
                                           )
                                           .as(
                                             acc + record.key.getBytes.length + record.value.getBytes.length + 68
                                           )
                                       }
                        } yield Info.Size(topicName, TopicSize(size))
                      }
        actual   <- runTopicInfoStream
      } yield assert(actual)(hasSubset(expected))
    }
  }

  val numRecordPropertyTestingSpec = test("should return record counts of topic") {
    check(Gen.mapOf(anyTopic, Gen.setOf(anyRecord))) { topicsWithRecords =>
      for {
        expected <- ZIO
                      .foreach(topicsWithRecords.toList) { case (topic, records) =>
                        for {
                          topicName   <- ZIO.serviceWithZIO[MakeTopicNameUnique](_.makeTopicNameUnique(topic.name))
                          _           <- KafkaUtils.createTopic(topicName)
                          recordCount <- ZIO.foldLeft(records)(0) { case (acc, record) =>
                                           KafkaUtils
                                             .produce(
                                               topic = topicName,
                                               key = record.key,
                                               value = record.value,
                                             )
                                             .as(
                                               acc + 1
                                             )
                                         }
                        } yield Info
                          .RecordCountInfo(topicName, RecordCount(recordCount))
                      }
        actual   <- runTopicInfoStream
      } yield assert(actual)(hasSubset(expected))
    }
  }

  val partitionCountPropertyTestingSpec = test("should return partition count") {
    check(Gen.setOf(anyTopic)) { topics =>
      for {
        expected <- ZIO
                      .foreach(topics) { topic =>
                        for {
                          topicName <- ZIO.serviceWithZIO[MakeTopicNameUnique](_.makeTopicNameUnique(topic.name))
                          _         <- KafkaUtils.createTopic(topicName, topic.numPartitions)
                        } yield Info
                          .PartitionInfo(topicName, Partition(topic.numPartitions))
                      }
        actual   <- runTopicInfoStream
      } yield assert(actual)(hasSubset(expected))
    }
  }

  val replicationFactorPropertyTestingSpec = test("should return replication factor count") {
    check(Gen.setOf(anyTopic)) { topics =>
      for {
        expected <- ZIO
                      .foreach(topics) { topic =>
                        for {
                          topicName <- ZIO.serviceWithZIO[MakeTopicNameUnique](_.makeTopicNameUnique(topic.name))
                          _         <- KafkaUtils.createTopic(
                                         topicName
                                       ) //can't create any other replication factor than 1 yet
                        } yield Info
                          .ReplicationFactorInfo(topicName, ReplicationFactor(1))
                      }
        actual   <- runTopicInfoStream
      } yield assert(actual)(hasSubset(expected))
    }
  }

  val spreadPropertyTestingSpec = test("should return spread") {
    check(Gen.setOf(anyTopic)) { topics =>
      for {
        expected <- ZIO
                      .foreach(topics) { topic =>
                        for {
                          topicName <- ZIO.serviceWithZIO[MakeTopicNameUnique](_.makeTopicNameUnique(topic.name))
                          _         <- KafkaUtils.createTopic(
                                         topicName
                                       ) //can't create any other spread than 1 yet
                        } yield Info
                          .SpreadInfo(topicName, Spread(1))
                      }
        actual   <- runTopicInfoStream
      } yield assert(actual)(hasSubset(expected))
    }
  }

  trait MakeTopicNameUnique {
    def makeTopicNameUnique(suffix: String): UIO[TopicName]
  }
  class MakeTopicNameUniqueLive(ref: Ref[Int]) extends MakeTopicNameUnique {
    override def makeTopicNameUnique(suffix: String): UIO[TopicName] = ref.getAndUpdate(_ + 1).map { prefixInt =>
      TopicName(s"$prefixInt-$suffix")
    }
  }
  object MakeTopicNameUniqueLive {
    def layer: ZLayer[Any, Nothing, MakeTopicNameUniqueLive] = ZLayer.fromZIO(Ref.make(0).map(new MakeTopicNameUniqueLive(_)))
  }

  val notSharedKafkaSuite = suite("not shared kafka")(
    shouldHandleNoTopicSpec,
    returnTopicsNamesSpec,
    sizeExampleSpec,
    sizePropertyTestingSpec,
    partitionCountPropertyTestingSpec,
  )

  val sharedKafkaSuite = suite("shared kafka")(
    numRecordPropertyTestingSpec,
    replicationFactorPropertyTestingSpec,
    spreadPropertyTestingSpec,
  )

  override def spec = suite("TopicInfoStreamServiceSpec")(
    suite("streamsInfo")(
      notSharedKafkaSuite.provide(
        KafkaTestContainer.kafkaLayer,
        KafkaServiceLive.layer,
        AdminClient.live,
        KafkaUtils.adminClientSettingsLayer,
        KafkaUtils.producerLayer,
        TopicInfoStreamServiceLive.layer,
        MakeTopicNameUniqueLive.layer,
      ) @@ sequential,
      sharedKafkaSuite.provideShared(
        KafkaTestContainer.kafkaLayer,
        KafkaServiceLive.layer,
        AdminClient.live,
        KafkaUtils.adminClientSettingsLayer,
        KafkaUtils.producerLayer,
        TopicInfoStreamServiceLive.layer,
        MakeTopicNameUniqueLive.layer,
      ),
    )
  ) @@ samples(3) @@ shrinks(0) @@ timeout(2.minutes)
}
