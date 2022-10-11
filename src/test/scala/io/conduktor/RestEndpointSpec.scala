package io.conduktor

import io.circe.{Encoder, Json, Printer}
import io.circe.generic.semiauto.deriveEncoder
import io.circe.literal.JsonStringContext
import io.conduktor.KafkaService.TopicName
import zhttp.http.{!!, HttpData, Method, Request, URL}
import zio.kafka.admin.AdminClient
import zio.test.{ZIOSpecDefault, _}
import zio.{Scope, Task, ZIO}

import java.nio.charset.Charset
import io.circe.parser.parse

object RestEndpointSpec extends ZIOSpecDefault {
  implicit class HttpDataExtension(httpData: HttpData) {
    def toJson: Task[Json] = httpData.toByteBuf.map(buffer =>
      parse(buffer.toString(Charset.forName("utf-8"))).toOption.get
    )
  }
  implicit val topicNameEncoder: Encoder[TopicName] =
    Encoder.encodeString.contramap[TopicName](_.value)

  private val allNameSpec = suite("/name")(
    test("should return topic name") {
      val topicName = TopicName("one")
      for {
        _ <- KafkaUtils.createTopic(topicName, numPartition = 1)
        app <- ZIO.service[RestEndpoints].map(_.app)
        response <- app(Request(url = URL(!! / "names")))
        responseBody <- response.data.toJson
      } yield assertTrue(responseBody == json"[$topicName]")
    }
  )

  private val describeTopicSpec = suite("/describe")(
    test("should describe topics") {
      val topicOne = TopicName("one")
      val topicTwo = TopicName("two")
      for {
        _ <- KafkaUtils.createTopic(topicOne, numPartition = 1)
        _ <- KafkaUtils.createTopic(topicTwo, numPartition = 2)
        app <- ZIO.service[RestEndpoints].map(_.app)
        response <- app(
          Request(url =
            URL(!! / "describe").setQueryParams(
              Map("topicNames" -> List(topicOne, topicTwo).map(_.value))
            )
          )
        )
        responseBody <- response.data.toJson
        _ <- ZIO.debug(responseBody.printWith(Printer.spaces2))
      } yield assertTrue(
        responseBody ==
          json"""{
                 "two" : {
                   "partition" : {
                     "0" : {
                       "leader" : 1,
                       "aliveReplicas" : [
                         1
                       ]
                     },
                     "1" : {
                       "leader" : 1,
                       "aliveReplicas" : [
                         1
                       ]
                     }
                   },
                   "replicationFactor" : 1
                 },
                 "one" : {
                   "partition" : {
                     "0" : {
                       "leader" : 1,
                       "aliveReplicas" : [
                         1
                       ]
                     }
                   },
                   "replicationFactor" : 1
                 }
               }"""
      )
    }
  )

  private val topicSizeSpec = suite("/size")(
    test("should return topics size") {
      val topicOne = TopicName("one")
      val topicTwo = TopicName("two")
      for {
        _ <- KafkaUtils.createTopic(topicOne, numPartition = 1)
        _ <- KafkaUtils.createTopic(topicTwo, numPartition = 1)
        _ <- KafkaUtils.produce(topicOne, "k", "v")
        app <- ZIO.service[RestEndpoints].map(_.app)
        response <- app(Request(url = URL(!! / "size")))
        responseBody <- response.data.toJson
      } yield assertTrue(
        responseBody ==
          json"""{"one":  70, "two": 0}"""
      )
    }
  )

  private val beginningOffsetSpec = suite("/offsets/begin")(
    test("should return topic first offset") {
      val topicOne = TopicName("one")
      val topicTwo = TopicName("two")
      for {
        _ <- KafkaUtils.createTopic(topicOne, numPartition = 1)
        _ <- KafkaUtils.createTopic(topicTwo, numPartition = 2)
        //TODO: maybe find a way to make one of the partition not starting at offset 0°
        app <- ZIO.service[RestEndpoints].map(_.app)
        response <- app(
          Request(
            url = URL(!! / "offsets" / "begin"),
            method = Method.GET,
            data = HttpData.fromString(
              json"""[{"topicName": "one", "partition": 0}, {"topicName": "two", "partition": 0}, {"topicName": "two", "partition": 1}]""".noSpaces,
              Charset.forName("utf-8")
            )
          )
        )
        //TODO: go for prettier solution
        responseBody <- response.data.toJson.map { json =>
          json.mapArray(
            _.sortBy { json =>
              val topicName = json.hcursor
                .get[String]("topicName")
                .getOrElse("")
              val partition = json.hcursor
                .get[Int](
                  "partition"
                )
                .getOrElse(0)
              topicName -> partition
            }
          )
        }
      } yield assertTrue(
        responseBody ==
          json"""[{"topicName": "one", "partition": 0, "offset": 0}, {"topicName": "two", "partition": 0, "offset": 0},
          {"topicName": "two", "partition": 1, "offset": 0}]"""
      )
    }
  )

  private val endOffsetSpec = suite("/offsets/end")(
    test("should return topic first offset") {
      val topicOne = TopicName("one")
      val topicTwo = TopicName("two")
      for {
        _ <- KafkaUtils.createTopic(topicOne, numPartition = 1)
        _ <- KafkaUtils.createTopic(topicTwo, numPartition = 2)
        _ <- KafkaUtils.produce(topicOne, "k", "v")
        app <- ZIO.service[RestEndpoints].map(_.app)
        response <- app(
          Request(
            url = URL(!! / "offsets" / "end"),
            method = Method.GET,
            data = HttpData.fromString(
              json"""[{"topicName": "one", "partition": 0}, {"topicName": "two", "partition": 0}, {"topicName": "two", "partition": 1}]""".noSpaces,
              Charset.forName("utf-8")
            )
          )
        )
        //TODO: go for prettier solution
        responseBody <- response.data.toJson.map { json =>
          json.mapArray(
            _.sortBy { json =>
              val topicName = json.hcursor
                .get[String]("topicName")
                .getOrElse("")
              val partition = json.hcursor
                .get[Int](
                  "partition"
                )
                .getOrElse(0)
              topicName -> partition
            }
          )
        }
      } yield assertTrue(
        responseBody ==
          json"""[{"topicName": "one", "partition": 0, "offset": 1}, {"topicName": "two", "partition": 0, "offset": 0},
          {"topicName": "two", "partition": 1, "offset": 0}]"""
      ) //FIXME: should not return partitions
    }
  )

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("RestEndpoint")(
      allNameSpec,
      describeTopicSpec,
      topicSizeSpec,
      beginningOffsetSpec,
      endOffsetSpec
    )
      .provide(
        RestEndpointsLive.layer,
        KafkaTestContainer.kafkaLayer,
        KafkaServiceLive.layer,
        AdminClient.live,
        KafkaUtils.adminClientSettingsLayer,
        KafkaUtils.producerLayer
      )
}
