package io.conduktor

import io.circe.{Encoder, Json, Printer}
import io.circe.generic.semiauto.deriveEncoder
import io.circe.literal.JsonStringContext
import io.conduktor.KafkaService.TopicName
import zhttp.http.{!!, HttpData, Method, Request, Status, URL}
import zio.kafka.admin.AdminClient
import zio.test.{ZIOSpecDefault, _}
import zio.{Scope, Task, ZIO}

import java.nio.charset.Charset
import io.circe.parser.parse
import zhttp.http.Status.BadRequest

object RestEndpointSpec extends ZIOSpecDefault {
  implicit class HttpDataExtension(httpData: HttpData) {
    def toJson: Task[Json] = httpData.toByteBuf.map(buffer => parse(buffer.toString(Charset.forName("utf-8"))).toOption.get)
  }
  implicit val topicNameEncoder: Encoder[TopicName] =
    Encoder.encodeString.contramap[TopicName](_.value)

  private val allNameSpec = suite("/name")(
    test("should return topic name") {
      val topicName = TopicName("one")
      for {
        _            <- KafkaUtils.createTopic(topicName, numPartition = 1)
        app          <- ZIO.service[RestEndpoints].map(_.app)
        response     <- app(Request(url = URL(!! / "names")))
        responseBody <- response.data.toJson
      } yield assertTrue(responseBody == json"[$topicName]")
    }
  )

  private val describeTopicSpec = suite("/describe")(
    test("should describe topics") {
      val topicOne = TopicName("one")
      val topicTwo = TopicName("two")
      for {
        _            <- KafkaUtils.createTopic(topicOne, numPartition = 1)
        _            <- KafkaUtils.createTopic(topicTwo, numPartition = 2)
        app          <- ZIO.service[RestEndpoints].map(_.app)
        response     <- app(
                          Request(url =
                            URL(!! / "describe").setQueryParams(
                              Map("topicNames" -> List(topicOne, topicTwo).map(_.value))
                            )
                          )
                        )
        responseBody <- response.data.toJson
        _            <- ZIO.debug(responseBody.printWith(Printer.spaces2))
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
        _            <- KafkaUtils.createTopic(topicOne, numPartition = 1)
        _            <- KafkaUtils.createTopic(topicTwo, numPartition = 1)
        _            <- KafkaUtils.produce(topicOne, "k", "v")
        app          <- ZIO.service[RestEndpoints].map(_.app)
        response     <- app(Request(url = URL(!! / "size")))
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
        _            <- KafkaUtils.createTopic(topicOne, numPartition = 1)
        _            <- KafkaUtils.createTopic(topicTwo, numPartition = 2)
        //TODO: maybe find a way to make one of the partition not starting at offset 0°
        app          <- ZIO.service[RestEndpoints].map(_.app)
        response     <- app(
                          Request(
                            url = URL(!! / "offsets" / "begin"),
                            method = Method.GET,
                            data = HttpData.fromString(
                              json"""[{"topicName": "one", "partition": 0}, {"topicName": "two", "partition": 0}, {"topicName": "two", "partition": 1}]""".noSpaces,
                              Charset.forName("utf-8"),
                            ),
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

  private val replicationFactorSpec =
    suite("/topics/{topic}/replicationFactor")(
      test("should return replication factor for a topic") {
        val topicName = TopicName("one")
        for {
          _            <- KafkaUtils.createTopic(
                            topicName,
                            numPartition = 1,
                            replicationFactor = 1,
                          )
          app          <- ZIO.service[RestEndpoints].map(_.app)
          response     <- app(
                            Request(
                              url = URL(!! / "topics" / topicName.value / "replicationFactor"),
                              method = Method.GET,
                            )
                          )
          responseBody <- response.data.toJson
        } yield assertTrue(responseBody == json"1")
      },
      test("should return 404 for replication factor on unknown topic") {
        val topicName = TopicName("one")
        for {
          app      <- ZIO.service[RestEndpoints].map(_.app)
          response <- app(
                        Request(
                          url = URL(!! / "topics" / topicName.value / "replicationFactor"),
                          method = Method.GET,
                        )
                      )
        } yield assertTrue(
          response.status == Status.BadRequest
        ) //TODO: change to notfound
      },
    )

  private val spreadSpec = suite("/topics/{topic}/spread")(
    test("should return spread size") {
      val topicName = TopicName("one")
      for {
        _            <- KafkaUtils.createTopic(
                          topicName,
                          numPartition = 1,
                          replicationFactor = 1,
                        )
        app          <- ZIO.service[RestEndpoints].map(_.app)
        response     <- app(
                          Request(
                            url = URL(!! / "topics" / topicName.value / "spread"),
                            method = Method.GET,
                          )
                        )
        responseBody <- response.data.toJson
      } yield assertTrue(responseBody == json"1.0")
    },
    test("should return 404 for unknown topic") {
      val topicName = TopicName("jenexistepas")
      for {
        app      <- ZIO.service[RestEndpoints].map(_.app)
        response <- app(
                      Request(
                        url = URL(!! / "topics" / topicName.value / "spread"),
                        method = Method.GET,
                      )
                    )
      } yield assertTrue(
        response.status == Status.BadRequest
      ) //TODO: change to notfound
    },
  )

  private val partitionCountSpec = suite("/topics/{topic}/partitions")(
    test("should return partition count for fields=count query param") {
      val topicName = TopicName("one")
      for {
        _            <- KafkaUtils.createTopic(topicName, numPartition = 5)
        app          <- ZIO.service[RestEndpoints].map(_.app)
        response     <- app(
                          Request(
                            url = URL(!! / "topics" / topicName.value / "partitions")
                              .setQueryParams("fields=count"),
                            method = Method.GET,
                          )
                        )
        responseBody <- response.data.toJson
      } yield assertTrue(responseBody == json"5")
    },
    test("should return 404 for replication factor on unknown topic") {
      val topicName = TopicName("one")
      for {
        app      <- ZIO.service[RestEndpoints].map(_.app)
        response <- app(
                      Request(
                        url = URL(!! / "topics" / topicName.value / "partitions")
                          .setQueryParams("fields=count"),
                        method = Method.GET,
                      )
                    )
      } yield assertTrue(
        response.status == Status.BadRequest
      ) //TODO: change to notfound
    },
    test("should fail when retrieving anything but count field") {
      val topicName = TopicName("one")
      for {
        _        <- KafkaUtils.createTopic(topicName, numPartition = 4)
        _        <- KafkaUtils.produce(topicName, "k", "v")
        _        <- KafkaUtils.produce(topicName, "k", "v")
        app      <- ZIO.service[RestEndpoints].map(_.app)
        response <- app(
                      Request(
                        url = URL(!! / "topics" / topicName.value / "partitions")
                          .setQueryParams("fields=payload"),
                        method = Method.GET,
                      )
                    )
      } yield assertTrue(response.status == BadRequest)
    },
  )

  private val recordCountSpec = suite("/topics/{topic}/records")(
    test("should return topic record count for fields=count query param") {
      val topicName = TopicName("one")
      for {
        _            <- KafkaUtils.createTopic(topicName, numPartition = 1)
        _            <- KafkaUtils.produce(topicName, "k", "v")
        _            <- KafkaUtils.produce(topicName, "k", "v")
        app          <- ZIO.service[RestEndpoints].map(_.app)
        response     <- app(
                          Request(
                            url = URL(!! / "topics" / topicName.value / "records")
                              .setQueryParams("fields=count"),
                            method = Method.GET,
                          )
                        )
        responseBody <- response.data.toJson
      } yield assertTrue(responseBody == json"2")
    },
    test("should fail when retrieving records") {
      val topicName = TopicName("one")
      for {
        _        <- KafkaUtils.createTopic(topicName, numPartition = 1)
        _        <- KafkaUtils.produce(topicName, "k", "v")
        _        <- KafkaUtils.produce(topicName, "k", "v")
        app      <- ZIO.service[RestEndpoints].map(_.app)
        response <- app(
                      Request(
                        url = URL(!! / "topics" / topicName.value / "records"),
                        method = Method.GET,
                      )
                    )
      } yield assertTrue(response.status == BadRequest)
    },
    test("should fail when retrieving anything but count field") {
      val topicName = TopicName("one")
      for {
        _        <- KafkaUtils.createTopic(topicName, numPartition = 1)
        _        <- KafkaUtils.produce(topicName, "k", "v")
        _        <- KafkaUtils.produce(topicName, "k", "v")
        app      <- ZIO.service[RestEndpoints].map(_.app)
        response <- app(
                      Request(
                        url = URL(!! / "topics" / topicName.value / "records")
                          .setQueryParams("fields=payload"),
                        method = Method.GET,
                      )
                    )
      } yield assertTrue(response.status == BadRequest)
    },
  )

  private val endOffsetSpec = suite("/offsets/end")(
    test("should return topic first offset") {
      val topicOne = TopicName("one")
      val topicTwo = TopicName("two")
      for {
        _            <- KafkaUtils.createTopic(topicOne, numPartition = 1)
        _            <- KafkaUtils.createTopic(topicTwo, numPartition = 2)
        _            <- KafkaUtils.produce(topicOne, "k", "v")
        app          <- ZIO.service[RestEndpoints].map(_.app)
        response     <- app(
                          Request(
                            url = URL(!! / "offsets" / "end"),
                            method = Method.GET,
                            data = HttpData.fromString(
                              json"""[{"topicName": "one", "partition": 0}, {"topicName": "two", "partition": 0}, {"topicName": "two", "partition": 1}]""".noSpaces,
                              Charset.forName("utf-8"),
                            ),
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
      )
    }
  )

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("RestEndpoint")(
      allNameSpec,
      describeTopicSpec,
      topicSizeSpec,
      beginningOffsetSpec,
      endOffsetSpec,
      recordCountSpec,
      replicationFactorSpec,
      partitionCountSpec,
      spreadSpec,
    )
      .provide(
        RestEndpointsLive.layer,
        KafkaTestContainer.kafkaLayer,
        KafkaServiceLive.layer,
        AdminClient.live,
        KafkaUtils.adminClientSettingsLayer,
        KafkaUtils.producerLayer,
      )
}
