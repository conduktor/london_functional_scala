package io.conduktor

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.literal.JsonStringContext
import io.conduktor.KafkaService.TopicName
import zhttp.http.{!!, Request, URL}
import zio.kafka.admin.AdminClient
import zio.test.{ZIOSpecDefault, _}
import zio.{Scope, ZIO}

import java.nio.charset.Charset
import io.circe.parser.parse

object RestEndpointSpec extends ZIOSpecDefault {
  implicit val topicNameEncoder: Encoder[TopicName] =
    Encoder.encodeString.contramap[TopicName](_.value)

  private val allNameSpec = suite("/name")(
    test("should return topic name") {
      val topicName = TopicName("one")
      for {
        _ <- KafkaUtils.createTopic(topicName, numPartition = 1)
        app <- ZIO.service[RestEndpoints].map(_.app)
        response <- app(Request(url = URL(!! / "names")))
        responseBody <-
          response.data.toByteBuf.map(buffer =>
            parse(buffer.toString(Charset.forName("utf-8"))).toOption.get
          )
      } yield assertTrue(responseBody == json"[$topicName]")
    }
  )

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("RestEndpoint")(allNameSpec)
      .provide(
        RestEndpointsLive.layer,
        KafkaTestContainer.kafkaLayer,
        KafkaServiceLive.layer,
        AdminClient.live,
        KafkaUtils.adminClientSettingsLayer
      )
}
