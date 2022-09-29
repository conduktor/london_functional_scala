package io.conduktor

import com.dimafeng.testcontainers.KafkaContainer
import io.conduktor.KafkaService._
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import zio._
import zio.kafka.admin.AdminClient.NewTopic
import zio.kafka.admin.{AdminClient, AdminClientSettings}
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serializer
import zio.test.Assertion._
import zio.test.TestAspect.{nondeterministic, samples, shrinks}
import zio.test._

object KafkaTestContainer {
  val kafkaLayer: ZLayer[Any, Nothing, KafkaContainer] =
    ZLayer.scoped {
      ZIO.acquireRelease(ZIO.attemptBlocking {
        val container = KafkaContainer()
        container.start()
        container
      }.orDie)(kafka => ZIO.attemptBlocking(kafka.stop()).orDie)
    }
}

object KafkaUtils {
  def produce(
      topic: TopicName,
      key: String,
      value: String
  ): ZIO[Producer, Throwable, Unit] =
    ZIO.serviceWithZIO[Producer](
      _.produce(
        topic.value,
        key = key,
        value = value,
        keySerializer = Serializer.string,
        valueSerializer = Serializer.string
      ).unit
    )

  val producerLayer =
    ZLayer.scoped {
      ZIO
        .serviceWith[KafkaContainer](container =>
          ProducerSettings(container.bootstrapServers :: Nil)
        )
        .flatMap(Producer.make)
    }
}

object KafkaAdmin {

  val adminClientSettings = ZLayer {
    ZIO.serviceWith[KafkaContainer](container =>
      AdminClientSettings(container.bootstrapServers :: Nil)
    )
  }

  def createTopic(
      name: TopicName,
      numPartition: Int = 3
  ): URIO[AdminClient, Unit] = ZIO
    .serviceWithZIO[AdminClient](
      _.createTopic(
        NewTopic(
          name = name.value,
          numPartitions = numPartition,
          replicationFactor = 1
        )
      )
    )
    .orDie
}

object KafkaServiceSpec extends ZIOSpecDefault {
  private val getTopicSizeSpec = suite("getTopicSize")(
    test("should return 1 for non empty topic partition") {
      val topicOne = TopicName("topicOne")
      val topicTwo = TopicName("topicTwo")
      val topicPartitionOne = TopicPartition(topicOne, Partition(0))
      val topicPartitionTwo = TopicPartition(topicTwo, Partition(0))
      check(Gen.stringBounded(1, 10)(Gen.char)) {aString =>
        for {
          _ <- ZIO.debug("starting test")
          _ <- KafkaAdmin.createTopic(name = topicOne, numPartition = 1)
          _ <- KafkaAdmin.createTopic(name = topicTwo, numPartition = 1)
          _ <- KafkaUtils.produce(topic = topicOne, key = "bar", value = aString)
          result <- ZIO.serviceWithZIO[KafkaService](
            _.getTopicSize(BrokerId(1))
          )
          _ <- ZIO.debug(result)
        } yield assertTrue(
          result == Map(topicPartitionOne -> TopicSize(aString.getBytes.length + "bar".getBytes.length + 68), topicPartitionTwo -> TopicSize(0))
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
        _ <- KafkaAdmin.createTopic(topicName)
        topics <- ZIO.serviceWithZIO[KafkaService](_.listTopicNames)
      } yield assertTrue(topics == Seq(topicName))
    }
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
        _ <- KafkaAdmin.createTopic(name = topicName1, numPartition = 3)
        _ <- KafkaAdmin.createTopic(name = topicName2, numPartition = 2)
        result <- ZIO.serviceWithZIO[KafkaService](
          _.describeTopics(Seq(topicName1, topicName2))
        )
        expected = Map(
          topicName1 -> TopicDescription(
            partition = Map(
              Partition(0) -> PartitionInfo(
                leader = Some(BrokerId(1)),
                aliveReplicas = List(BrokerId(1))
              ),
              Partition(1) -> PartitionInfo(
                leader = Some(BrokerId(1)),
                aliveReplicas = List(BrokerId(1))
              ),
              Partition(2) -> PartitionInfo(
                leader = Some(BrokerId(1)),
                aliveReplicas = List(BrokerId(1))
              )
            ),
            replicationFactor = 1
          ),
          topicName2 -> TopicDescription(
            partition = Map(
              Partition(0) -> PartitionInfo(
                leader = Some(BrokerId(1)),
                aliveReplicas = List(BrokerId(1))
              ),
              Partition(1) -> PartitionInfo(
                leader = Some(BrokerId(1)),
                aliveReplicas = List(BrokerId(1))
              )
            ),
            replicationFactor = 1
          )
        )
      } yield assertTrue(result == expected)
    }
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
      val topicName = TopicName("yo")
      val topicPartition = TopicPartition(topicName, Partition(0))
      for {
        _ <- KafkaAdmin.createTopic(name = topicName, numPartition = 1)
        result <- ZIO.serviceWithZIO[KafkaService](
          _.beginningOffsets(Seq(topicPartition))
        )
      } yield assertTrue(
        result == Map(topicPartition -> Offset(0))
      )
    },
    test("should return 0 for non empty topic partition") {
      val topicName = TopicName("ya")
      val topicPartition = TopicPartition(topicName, Partition(0))
      for {
        _ <- KafkaAdmin.createTopic(name = topicName, numPartition = 1)
        _ <- KafkaUtils.produce(topic = topicName, key = "bar", value = "foo")
        result <- ZIO.serviceWithZIO[KafkaService](
          _.beginningOffsets(Seq(topicPartition))
        )
      } yield assertTrue(
        result == Map(topicPartition -> Offset(0))
      )
    }
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
      val topicName = TopicName("yoyo")
      val topicPartition = TopicPartition(topicName, Partition(0))
      for {
        _ <- KafkaAdmin.createTopic(name = topicName, numPartition = 1)
        result <- ZIO.serviceWithZIO[KafkaService](
          _.endOffsets(Seq(topicPartition))
        )
      } yield assertTrue(
        result == Map(topicPartition -> Offset(0))
      )
    },
    test("should return 1 for non empty topic partition") {
      val topicName = TopicName("yaya")
      val topicPartition = TopicPartition(topicName, Partition(0))
      for {
        _ <- KafkaAdmin.createTopic(name = topicName, numPartition = 1)
        _ <- KafkaUtils.produce(topic = topicName, key = "bar", value = "foo")
        result <- ZIO.serviceWithZIO[KafkaService](
          _.endOffsets(Seq(topicPartition))
        )
      } yield assertTrue(
        result == Map(topicPartition -> Offset(1))
      )
    }
  )

  override def spec = suite("KafkaService")(
      suite("not shared kafka")(/*listTopicsSpec, */getTopicSizeSpec).provide(
        KafkaTestContainer.kafkaLayer,
        KafkaServiceLive.layer,
        AdminClient.live,
        KafkaAdmin.adminClientSettings,
        KafkaUtils.producerLayer,
      ),
    suite("shared kafka")(
      describeTopicsSpec,
      beginningOffsetsSpec,
      endOffsetsSpec,
    )
      .provideShared(
        KafkaTestContainer.kafkaLayer,
        KafkaServiceLive.layer,
        AdminClient.live,
        KafkaAdmin.adminClientSettings,
        KafkaUtils.producerLayer
      )
  )
}
