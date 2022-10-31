package io.conduktor.v2

import io.conduktor.KafkaService.{TopicName, TopicSize}
import io.conduktor.TopicInfoStreamService.Info
import io.conduktor.TopicInfoStreamServiceSpec.MakeTopicNameUniqueLive
import io.conduktor.v2.Input.Command
import io.conduktor.{KafkaServiceLive, KafkaTestContainer, KafkaUtils}
import zio._
import zio.kafka.admin.AdminClient
import zio.stream.ZStream
import zio.test.Assertion.equalTo
import zio.test.TestAspect.{samples, sequential, shrinks, timeout}
import zio.test._

object TopicInfoPaginatedStreamServiceSpec extends ZIOSpecDefault {

  def runTopicInfoStream(pageSize: Int, totalTopics: Int) = {
    val totalPages = (totalTopics - 1) / pageSize + 1
    for {
      queue  <- Queue.unbounded[Command]
      _      <- queue.offer(Command.Subscribe(1))
      result <-
        ZStream
          .serviceWithStream[TopicInfoPaginatedStreamService](
            _.streamInfos(pageSize, queue)
          )
          .mapAccumZIO((1, List.empty[Info])) { case ((pageNum, currentPageContent), info) =>
            info match {
              case Info.Complete =>
                val newPageNum = pageNum + 1
                queue
                  .offer(Command.Subscribe(newPageNum))
                  .as(
                    (newPageNum -> List.empty[Info], List(currentPageContent))
                  )
              case others        =>
                ZIO.succeed(pageNum -> (currentPageContent :+ others), List.empty)
            }
          }
          .mapConcat(identity)
          .take(totalPages)
          .runCollect
    } yield result
  }

  val shouldHandleNoTopicSpec = test("should terminate even when no topics") {
    assertZIO(runTopicInfoStream(42, 0))(equalTo(Chunk(Nil)))
  }

  val returnTopicsNamesSpec =
    test("should first return some topic names") {
      val topicOne   = TopicName("topic1")
      val topicTwo   = TopicName("topic2")
      val topicThree = TopicName("topic3")
      val topicFour  = TopicName("topic4")
      for {
        _      <- KafkaUtils.createTopic(name = topicOne)
        _      <- KafkaUtils.createTopic(name = topicTwo)
        _      <- KafkaUtils.createTopic(name = topicThree)
        _      <- KafkaUtils.createTopic(name = topicFour)
        result <- runTopicInfoStream(pageSize = 3, totalTopics = 4)
        names   = result.map(_.collect { case info: Info.Topics =>
                    info
                  })
      } yield assert(names)(equalTo(Chunk(List(Info.Topics(Set(topicOne, topicTwo, topicThree))), List(Info.Topics(Set(topicFour))))))
    }

  val sizeExampleSpec = test("should return size of topics - example") {
    val topicOne   = TopicName("topic1")
    val topicTwo   = TopicName("topic2")
    val topicThree = TopicName("topic3")
    val topicFour  = TopicName("topic4")
    for {
      _      <- KafkaUtils.createTopic(name = topicOne)
      _      <- KafkaUtils.produce(topic = topicOne, key = "bar", value = "foo1")
      _      <- KafkaUtils.produce(topic = topicOne, key = "bar", value = "foo2")
      _      <- KafkaUtils.createTopic(name = topicTwo)
      _      <- KafkaUtils.produce(topic = topicTwo, key = "bar", value = "foo1")
      _      <- KafkaUtils.produce(topic = topicTwo, key = "bar", value = "foo2")
      _      <- KafkaUtils.produce(topic = topicTwo, key = "bar", value = "foo3")
      _      <- KafkaUtils.createTopic(name = topicThree)
      _      <- KafkaUtils.produce(topic = topicThree, key = "bar", value = "foo1")
      _      <- KafkaUtils.produce(topic = topicThree, key = "bar", value = "foo2")
      _      <- KafkaUtils.produce(topic = topicThree, key = "bar", value = "foo3")
      _      <- KafkaUtils.produce(topic = topicThree, key = "bar", value = "foo4")
      _      <- KafkaUtils.createTopic(name = topicFour)
      result <- runTopicInfoStream(pageSize = 2, 4)
      infos   = result.map(_.collect { case size: Info.Size =>
                  size
                }.toSet)
    } yield assert(infos)(
      equalTo(
        Chunk(
          Set(
            Info.Size(topicOne, TopicSize(150)),
            Info.Size(topicTwo, TopicSize(225)),
          ),
          Set(
            Info.Size(topicThree, TopicSize(300)),
            Info.Size(topicFour, TopicSize(0)),
          ),
        )
      )
    )
  }

  //TODO: write others tests

  override def spec = suite("TopicInfoPaginatedStreamService")(
    suite("streamsInfo")(
      suite("not shared kafka")(
        shouldHandleNoTopicSpec,
        returnTopicsNamesSpec,
        sizeExampleSpec,
      ).provide(
        KafkaTestContainer.kafkaLayer,
        KafkaServiceLive.layer,
        AdminClient.live,
        KafkaUtils.adminClientSettingsLayer,
        KafkaUtils.producerLayer,
        TopicInfoPaginatedStreamServiceLive.layer,
        MakeTopicNameUniqueLive.layer,
      ) @@ sequential,
      suite("shared kafka")().provideShared(
        KafkaTestContainer.kafkaLayer,
        KafkaServiceLive.layer,
        AdminClient.live,
        KafkaUtils.adminClientSettingsLayer,
        KafkaUtils.producerLayer,
        TopicInfoPaginatedStreamServiceLive.layer,
        MakeTopicNameUniqueLive.layer,
      ),
    )
  ) @@ samples(3) @@ shrinks(0) @@ timeout(60.second)
}
