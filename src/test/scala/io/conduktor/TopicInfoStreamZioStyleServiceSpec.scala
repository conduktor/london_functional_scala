package io.conduktor

import io.conduktor.TopicInfoStreamServiceSpec.{MakeTopicNameUniqueLive, notSharedKafkaSuite, sharedKafkaSuite}
import zio._
import zio.kafka.admin.AdminClient
import zio.test.TestAspect.{samples, sequential, shrinks, timeout}
import zio.test._

object TopicInfoStreamZioStyleServiceSpec extends ZIOSpecDefault {

  override def spec = suite("TopicInfoStreamServiceSpec")(
    suite("streamsInfo")(
      notSharedKafkaSuite.provide(
        KafkaTestContainer.kafkaLayer,
        KafkaServiceLive.layer,
        AdminClient.live,
        KafkaUtils.adminClientSettingsLayer,
        KafkaUtils.producerLayer,
        TopicInfoStreamZioStyleServiceLive.layer,
        MakeTopicNameUniqueLive.layer,
      ) @@ sequential,
      sharedKafkaSuite.provideShared(
        KafkaTestContainer.kafkaLayer,
        KafkaServiceLive.layer,
        AdminClient.live,
        KafkaUtils.adminClientSettingsLayer,
        KafkaUtils.producerLayer,
        TopicInfoStreamZioStyleServiceLive.layer,
        MakeTopicNameUniqueLive.layer,
      ),
    )
  ) @@ samples(3) @@ shrinks(0) @@ timeout(2.minutes)
}
