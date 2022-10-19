package io.conduktor

import io.conduktor.KafkaService._
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import zio._
import zio.kafka.admin.AdminClient
import zio.kafka.admin.AdminClient.NewTopic
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test.TestAspect.{nondeterministic, samples, shrinks}
import zio.test._

object KafkaServiceSpec extends ZIOSpecDefault {

  case class Record(key: String, value: String)

  val anyTopic: Gen[Any, NewTopic] =
    (Gen.alphaNumericString.filter(!_.isBlank).map(TopicName) <*> Gen.small(size => Gen.const(TopicSize(size))))
      .map { case (name, size) =>
        NewTopic(name.value, size.value.toInt + 1, replicationFactor = 1)
      }

  val anyRecord: Gen[Any, Record] =
    (Gen.alphaNumericString <*> Gen.alphaNumericString).map { case (key, value) =>
      Record(key, value)
    }

  private val getTopicSizeSpec = suite("getTopicSize")(
    test("should return 1 for non empty topic partition") {
      val topicOne = TopicName("topicOne")
      val topicTwo = TopicName("topicTwo")
      check(Gen.stringBounded(1, 10)(Gen.char)) { aString =>
        for {
          _      <- KafkaUtils.createTopic(name = topicOne, numPartition = 1)
          _      <- KafkaUtils.createTopic(name = topicTwo, numPartition = 1)
          _      <- KafkaUtils.produce(
                      topic = topicOne,
                      key = "bar",
                      value = aString,
                    )
          result <- ZIO.serviceWithZIO[KafkaService](
                      _.getTopicSize
                    )
        } yield assertTrue(
          result == Map(
            topicOne -> TopicSize(
              aString.getBytes.length + "bar".getBytes.length + 68
            ),
            topicTwo -> TopicSize(0),
          )
        )
      }
    } @@ samples(1) @@ nondeterministic @@ shrinks(0)
  )

  private val listTopicsSpec = suite("listTopics")(
    test("should list topics when empty") {
      for {
        topics <- ZIO.serviceWithZIO[KafkaService](_.listTopicNames)
      } yield assertTrue(topics.isEmpty)
    },
    test("should list topics") {
      val topicName = TopicName("foo")
      for {
        _      <- KafkaUtils.createTopic(topicName)
        topics <- ZIO.serviceWithZIO[KafkaService](_.listTopicNames)
      } yield assertTrue(topics == Seq(topicName))
    },
  )

  private val describeTopicsSpec = suite("describeTopics")(
    test("empty input topic list should return empty result") {
      for {
        result <- ZIO.serviceWithZIO[KafkaService](_.describeTopics(Seq.empty))
      } yield assertTrue(result == Map.empty[TopicName, TopicDescription])
    },
    test("should properly describe two topics") {
      val topicName1 = TopicName("one")
      val topicName2 = TopicName("two")
      for {
        _       <- KafkaUtils.createTopic(name = topicName1, numPartition = 3)
        _       <- KafkaUtils.createTopic(name = topicName2, numPartition = 2)
        result  <- ZIO.serviceWithZIO[KafkaService](
                     _.describeTopics(Seq(topicName1, topicName2))
                   )
        expected = Map(
                     topicName1 -> TopicDescription(
                       partition = Map(
                         Partition(0) -> PartitionInfo(
                           leader = Some(BrokerId(1)),
                           aliveReplicas = List(BrokerId(1)),
                         ),
                         Partition(1) -> PartitionInfo(
                           leader = Some(BrokerId(1)),
                           aliveReplicas = List(BrokerId(1)),
                         ),
                         Partition(2) -> PartitionInfo(
                           leader = Some(BrokerId(1)),
                           aliveReplicas = List(BrokerId(1)),
                         ),
                       ),
                       replicationFactor = ReplicationFactor(1),
                     ),
                     topicName2 -> TopicDescription(
                       partition = Map(
                         Partition(0) -> PartitionInfo(
                           leader = Some(BrokerId(1)),
                           aliveReplicas = List(BrokerId(1)),
                         ),
                         Partition(1) -> PartitionInfo(
                           leader = Some(BrokerId(1)),
                           aliveReplicas = List(BrokerId(1)),
                         ),
                       ),
                       replicationFactor = ReplicationFactor(1),
                     ),
                   )
      } yield assertTrue(result == expected)
    },
  )

  private val beginningOffsetsSpec = suite("beginningOffsets")(
    test("should fail on unknown partition") {
      for {
        result <- ZIO
                    .serviceWithZIO[KafkaService](
                      _.beginningOffsets(
                        Seq(TopicPartition(TopicName("topicnambur"), Partition(1)))
                      )
                    )
                    .exit
      } yield assert(result)(
        fails(isSubtype[UnknownTopicOrPartitionException](anything))
      )
    },
    test("should return 0 for empty topic partition") {
      val topicName      = TopicName("yo")
      val topicPartition = TopicPartition(topicName, Partition(0))
      for {
        _      <- KafkaUtils.createTopic(name = topicName, numPartition = 1)
        result <- ZIO.serviceWithZIO[KafkaService](
                    _.beginningOffsets(Seq(topicPartition))
                  )
      } yield assertTrue(
        result == Map(topicPartition -> Offset(0))
      )
    },
    test("should return 0 for non empty topic partition") {
      val topicName      = TopicName("ya")
      val topicPartition = TopicPartition(topicName, Partition(0))
      for {
        _      <- KafkaUtils.createTopic(name = topicName, numPartition = 1)
        _      <- KafkaUtils.produce(topic = topicName, key = "bar", value = "foo")
        result <- ZIO.serviceWithZIO[KafkaService](
                    _.beginningOffsets(Seq(topicPartition))
                  )
      } yield assertTrue(
        result == Map(topicPartition -> Offset(0))
      )
    },
  )

  private val endOffsetsSpec = suite("endOffsets")(
    test("should fail on unknown partition") {
      for {
        result <- ZIO
                    .serviceWithZIO[KafkaService](
                      _.endOffsets(
                        Seq(TopicPartition(TopicName("topicnambur"), Partition(1)))
                      )
                    )
                    .exit
      } yield assert(result)(
        fails(isSubtype[UnknownTopicOrPartitionException](anything))
      )
    },
    test("should return 0 for empty topic partition") {
      val topicName      = TopicName("yoyo")
      val topicPartition = TopicPartition(topicName, Partition(0))
      for {
        _      <- KafkaUtils.createTopic(name = topicName, numPartition = 1)
        result <- ZIO.serviceWithZIO[KafkaService](
                    _.endOffsets(Seq(topicPartition))
                  )
      } yield assertTrue(
        result == Map(topicPartition -> Offset(0))
      )
    },
    test("should return 1 for non empty topic partition") {
      val topicName      = TopicName("yaya")
      val topicPartition = TopicPartition(topicName, Partition(0))
      for {
        _      <- KafkaUtils.createTopic(name = topicName, numPartition = 1)
        _      <- KafkaUtils.produce(topic = topicName, key = "bar", value = "foo")
        result <- ZIO.serviceWithZIO[KafkaService](
                    _.endOffsets(Seq(topicPartition))
                  )
      } yield assertTrue(
        result == Map(topicPartition -> Offset(1))
      )
    },
  )

  private val recordCountSpec = suite("recordCount")(
    test("should return none for non existing topic") {
      ZIO
        .serviceWithZIO[KafkaService](
          _.recordCount(TopicName("i_dont_exist")).either
        )
        .map(result => assert(result)(isLeft(anything)))
    },
    test("should return zero for an empty topic") {
      val topicName = TopicName("recordCountTest4")
      for {
        _      <- KafkaUtils.createTopic(name = topicName, numPartition = 1)
        result <- ZIO.serviceWithZIO[KafkaService](
                    _.recordCount(topicName)
                  )
      } yield assertTrue(
        result == RecordCount(0)
      )
    },
    test("should return the num of record for a topic") {
      val topicName = TopicName("recordCountTest2")
      for {
        _      <- KafkaUtils.createTopic(name = topicName, numPartition = 3)
        _      <- KafkaUtils.produce(topic = topicName, key = "bar", value = "foo")
        _      <- KafkaUtils.produce(topic = topicName, key = "bar", value = "foo")
        _      <- KafkaUtils.produce(topic = topicName, key = "bar", value = "foo")
        _      <- KafkaUtils.produce(topic = topicName, key = "bar", value = "foo")
        _      <- KafkaUtils.produce(topic = topicName, key = "bar", value = "foo")
        _      <- KafkaUtils.produce(topic = topicName, key = "bar", value = "foo")
        result <- ZIO.serviceWithZIO[KafkaService](
                    _.recordCount(topicName)
                  )
      } yield assertTrue(
        result == RecordCount(6)
      )
    },
  )

  private val brokerCountSpec = suite("brokerCount")(
    test("should return one with kafka having one node") {
      ZIO
        .serviceWithZIO[KafkaService](
          _.brokerCount
        )
        .map { brokerCount =>
          assertTrue(brokerCount == BrokerCount(1))
        }
    }
  )

  private val spreadSpec = suite("spread")(
    test("should return spread of a partition") {
      val topicName = TopicName("spreadSpecTopicForTest")
      for {
        -      <- KafkaUtils.createTopic(topicName, numPartition = 3)
        spread <- ZIO
                    .serviceWithZIO[KafkaService](
                      _.topicSpread(topicName)
                    )
      } yield assertTrue(spread == Spread(1.0))
    },
    test("should fail when topic does not exist") {
      assertZIO(
        ZIO
          .serviceWithZIO[KafkaService](
            _.topicSpread(TopicName("jenexistepas"))
          )
          .either
      )(isLeft(isSubtype[UnknownTopicOrPartitionException](anything)))
    },
  )

  private val infoSpec = suite("info")(
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
                   .serviceWithStream[KafkaService](
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
    },
    test("should return size of topics - example") {
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
        infos <- ZStream
                   .serviceWithStream[KafkaService](
                     _.streamInfos
                   )
                   .debug("info stream : ")
                   .runCollect
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
    },
    suite("property testing")(
      test("should return size of topics") {
        Ref.make(0).flatMap { topicPrefix =>
          check(Gen.mapOf(anyTopic, Gen.setOf(anyRecord))) { topicsWithRecords =>
            for {
              expected <- ZIO
                            .foreach(topicsWithRecords.toList) { case (topic, records) =>
                              for {
                                topicName <- topicPrefix.getAndUpdate(_ + 1).map { prefix =>
                                               TopicName(s"$prefix-${topic.name}")
                                             }
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
              actual   <- ZStream
                            .serviceWithStream[KafkaService](
                              _.streamInfos
                            )
                            .runCollect
                            .map(_.toList)
            } yield assert(actual)(hasSubset(expected))
          }
        }
      },
      test("should return record counts of topic") {
        Ref.make(0).flatMap { topicPrefix =>
          check(Gen.mapOf(anyTopic, Gen.setOf(anyRecord))) { topicsWithRecords =>
            for {
              expected <- ZIO
                            .foreach(topicsWithRecords.toList) { case (topic, records) =>
                              for {
                                topicName   <- topicPrefix.getAndUpdate(_ + 1).map { prefix =>
                                                 TopicName(s"$prefix-${topic.name}")
                                               }
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
              actual   <- ZStream
                            .serviceWithStream[KafkaService](
                              _.streamInfos
                            )
                            .runCollect
                            .map(_.toList)
            } yield assert(actual)(hasSubset(expected))
          }
        }
      },
      test("should return partition count") {
        Ref.make(0).flatMap { topicPrefix =>
          check(Gen.setOf(anyTopic)) { topics =>
            for {
              expected <- ZIO
                            .foreach(topics) { topic =>
                              for {
                                topicName <- topicPrefix.getAndUpdate(_ + 1).map { prefix =>
                                               TopicName(s"$prefix-${topic.name}")
                                             }
                                _         <- KafkaUtils.createTopic(topicName, topic.numPartitions)
                              } yield Info
                                .PartitionInfo(topicName, Partition(topic.numPartitions))
                            }
              actual   <- ZStream
                            .serviceWithStream[KafkaService](
                              _.streamInfos
                            )
                            .runCollect
                            .map(_.toList)
            } yield assert(actual)(hasSubset(expected))
          }
        }
      },
      test("should return replication factor count") {
        Ref.make(0).flatMap { topicPrefix =>
          check(Gen.setOf(anyTopic)) { topics =>
            for {
              expected <- ZIO
                            .foreach(topics) { topic =>
                              for {
                                topicName <- topicPrefix.getAndUpdate(_ + 1).map { prefix =>
                                               TopicName(s"$prefix-${topic.name}")
                                             }
                                _         <- KafkaUtils.createTopic(
                                               topicName
                                             ) //can't create any other replication factor than 1 yet
                              } yield Info
                                .ReplicationFactorInfo(topicName, ReplicationFactor(1))
                            }
              actual   <- ZStream
                            .serviceWithStream[KafkaService](
                              _.streamInfos
                            )
                            .runCollect
                            .map(_.toList)
            } yield assert(actual)(hasSubset(expected))
          }
        }
      },
      test("should return spread") {
        Ref.make(0).flatMap { topicPrefix =>
          check(Gen.setOf(anyTopic)) { topics =>
            for {
              expected <- ZIO
                            .foreach(topics) { topic =>
                              for {
                                topicName <- topicPrefix.getAndUpdate(_ + 1).map { prefix =>
                                               TopicName(s"$prefix-${topic.name}")
                                             }
                                _         <- KafkaUtils.createTopic(
                                               topicName
                                             ) //can't create any other spread than 1 yet
                              } yield Info
                                .SpreadInfo(topicName, Spread(1))
                            }
              actual   <- ZStream
                            .serviceWithStream[KafkaService](
                              _.streamInfos
                            )
                            .runCollect
                            .map(_.toList)
            } yield assert(actual)(hasSubset(expected))
          }
        }
      },
    ) @@ samples(10) @@ shrinks(0),
  )

  override def spec = suite("KafkaService")(
    suite("not shared kafka")(
      listTopicsSpec,
      getTopicSizeSpec,
      brokerCountSpec,
      infoSpec,
    ).provide(
      KafkaTestContainer.kafkaLayer,
      KafkaServiceLive.layer,
      AdminClient.live,
      KafkaUtils.adminClientSettingsLayer,
      KafkaUtils.producerLayer,
    ),
    suite("shared kafka")(
      recordCountSpec,
      describeTopicsSpec,
      beginningOffsetsSpec,
      endOffsetsSpec,
      brokerCountSpec,
      spreadSpec,
    )
      .provideShared(
        KafkaTestContainer.kafkaLayer,
        KafkaServiceLive.layer,
        AdminClient.live,
        KafkaUtils.adminClientSettingsLayer,
        KafkaUtils.producerLayer,
      ),
  )
}
