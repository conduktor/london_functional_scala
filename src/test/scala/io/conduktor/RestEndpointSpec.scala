package io.conduktor

import io.circe
import io.circe.{Encoder, Json}
import io.circe.literal.JsonStringContext
import io.conduktor.KafkaService.TopicName
import io.conduktor.v2.TopicInfoPaginatedStreamServiceLive
import zio.kafka.admin.AdminClient
import zio.test.{ZIOSpecDefault, _}
import zio.{Scope, Task, ZIO}
import org.http4s.server.Server
import org.http4s
import sttp.client3._
import sttp.client3.circe._
import sttp.client3.httpclient.zio.HttpClientZioBackend
import sttp.model.{StatusCode, Uri}

object RestEndpointSpec extends ZIOSpecDefault {

  val baseUri: Uri = uri"http://localhost:8090"

  implicit def http4sToSttp(in: http4s.Uri): Uri = Uri.unsafeParse(in.renderString)

  def sendHttpRequest(
    request: http4s.Uri => Request[Either[String, String], Any]
  ): ZIO[SttpBackend[Task, Any] with Server, Throwable, Response[Either[ResponseException[String, circe.Error], Json]]] =
    for {
      httpClient <- ZIO.service[SttpBackend[Task, Any]]
      baseUri    <- ZIO.serviceWith[Server](_.baseUri)
      response   <- httpClient.send(request(baseUri).response(asJson[Json]))
    } yield response

  implicit val topicNameEncoder: Encoder[TopicName] =
    Encoder.encodeString.contramap[TopicName](_.value)

  private val allNameSpec = suite("/name")(
    test("should return topic name") {
      val topicName = TopicName("one")
      for {
        _        <- KafkaUtils.createTopic(topicName, numPartition = 1)
        response <- sendHttpRequest(baseUri => emptyRequest.get(baseUri / "names"))
      } yield assertTrue(response.body == Right(json"[$topicName]"))
    }
  )

  private val describeTopicSpec = suite("/describe")(
    test("should describe topics") {
      val topicOne = TopicName("describeTopicSpecOne")
      val topicTwo = TopicName("describeTopicSpecTwo")
      for {
        _        <- KafkaUtils.createTopic(topicOne, numPartition = 1)
        _        <- KafkaUtils.createTopic(topicTwo, numPartition = 2)
        response <- sendHttpRequest(baseUri =>
                      emptyRequest.get((baseUri / "describe").setQueryParams(Map("topicNames" -> List(topicOne, topicTwo).map(_.value))))
                    )
      } yield assertTrue(
        response.body ==
          Right(json"""{
                 ${topicTwo.value} : {
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
                 ${topicOne.value} : {
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
               }""")
      )
    }
  )

  private val topicSizeSpec = suite("/size")(
    test("should return topics size") {
      val topicOne = TopicName("one")
      val topicTwo = TopicName("two")
      for {
        _        <- KafkaUtils.createTopic(topicOne, numPartition = 1)
        _        <- KafkaUtils.createTopic(topicTwo, numPartition = 1)
        _        <- KafkaUtils.produce(topicOne, "k", "v")
        response <- sendHttpRequest(baseUri => emptyRequest.get(baseUri / "size"))
      } yield assertTrue(
        response.body ==
          Right(json"""{"one":  70, "two": 0}""")
      )
    }
  )

  private val beginningOffsetSpec   = suite("/offsets/begin")(
    test("should return topic first offset") {
      val topicOne = TopicName("beginningOffsetOne")
      val topicTwo = TopicName("beginningOffsetTwo")
      for {
        _        <- KafkaUtils.createTopic(topicOne, numPartition = 1)
        _        <- KafkaUtils.createTopic(topicTwo, numPartition = 2)
        //TODO: maybe find a way to make one of the partition not starting at offset 0°
        response <-
          sendHttpRequest(baseUri =>
            emptyRequest
              .get(baseUri / "offsets" / "begin")
              .body(
                json"""[{"topicName": ${topicOne.value}, "partition": 0}, {"topicName": ${topicTwo.value}, "partition": 0}, {"topicName": ${topicTwo.value}, "partition": 1}]"""
              )
          )
        //TODO: go for prettier solution
        actual    = response.body.map { json =>
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
        actual ==
          Right(json"""[{"topicName": ${topicOne.value}, "partition": 0, "offset": 0}, {"topicName": ${topicTwo.value}, "partition": 0, "offset": 0},
          {"topicName": ${topicTwo.value}, "partition": 1, "offset": 0}]""")
      )
    }
  )
  private val replicationFactorSpec =
    suite("/topics/{topic}/replicationFactor")(
      test("should return replication factor for a topic") {
        val topicName = TopicName("replicationFactorSpecTopic")
        for {
          _        <- KafkaUtils.createTopic(
                        topicName,
                        numPartition = 1,
                        replicationFactor = 1,
                      )
          response <- sendHttpRequest(baseUri => emptyRequest.get(baseUri / "topics" / topicName.value / "replicationFactor"))
        } yield assertTrue(response.body == Right(json"1"))
      },
      test("should return 404 for replication factor on unknown topic") {
        val topicName = TopicName("jenexistepas")
        for {
          response <- sendHttpRequest(baseUri => emptyRequest.get(baseUri / "topics" / topicName.value / "replicationFactor"))
        } yield assertTrue(
          response.code == StatusCode.BadRequest
        ) //TODO: change to notfound
      },
    )

  private val spreadSpec         = suite("/topics/{topic}/spread")(
    test("should return spread size") {
      val topicName = TopicName("spreadSpecOne")
      for {
        _        <- KafkaUtils.createTopic(
                      topicName,
                      numPartition = 1,
                      replicationFactor = 1,
                    )
        response <- sendHttpRequest(baseUri => emptyRequest.get(baseUri / "topics" / topicName.value / "spread"))
      } yield assertTrue(response.body == Right(json"1.0"))
    },
    test("should return 404 for unknown topic") {
      val topicName = TopicName("jenexistepas")
      for {
        response <- sendHttpRequest(baseUri => emptyRequest.get(baseUri / "topics" / topicName.value / "spread"))
      } yield assertTrue(
        response.code == StatusCode.BadRequest
      ) //TODO: change to notfound
    },
  )
  private val partitionCountSpec = suite("/topics/{topic}/partitions")(
    test("should return partition count for fields=count query param") {
      val topicName = TopicName("partitionCountSpecTopic")
      for {
        _        <- KafkaUtils.createTopic(topicName, numPartition = 5)
        response <-
          sendHttpRequest(baseUri =>
            emptyRequest.get((baseUri / "topics" / topicName.value / "partitions").withQueryParam("fields", "count"))
          )
      } yield assertTrue(response.body == Right(json"5"))
    },
    test("should return 404 for replication factor on unknown topic") {
      val topicName = TopicName("jenexistepas")
      for {
        response <-
          sendHttpRequest(baseUri =>
            emptyRequest.get((baseUri / "topics" / topicName.value / "partitions").withQueryParam("fields", "count"))
          )
      } yield assertTrue(
        response.code == StatusCode.BadRequest
      ) //TODO: change to notfound
    },
    test("should fail when retrieving anything but count field") {
      val topicName = TopicName("one")
      for {
        _        <- KafkaUtils.createTopic(topicName, numPartition = 4)
        _        <- KafkaUtils.produce(topicName, "k", "v")
        _        <- KafkaUtils.produce(topicName, "k", "v")
        response <- sendHttpRequest(baseUri =>
                      emptyRequest
                        .get(
                          (baseUri / "topics" / topicName.value / "partitions")
                            .withQueryParam("fields", "payload")
                        )
                    )
      } yield assertTrue(response.code == StatusCode.BadRequest)
    },
  )
  private val recordCountSpec    = suite("/topics/{topic}/records")(
    test("should return topic record count for fields=count query param") {
      val topicName = TopicName("recordCountSpecTopicOne")
      for {
        _        <- KafkaUtils.createTopic(topicName, numPartition = 1)
        _        <- KafkaUtils.produce(topicName, "k", "v")
        _        <- KafkaUtils.produce(topicName, "k", "v")
        response <-
          sendHttpRequest(baseUri => emptyRequest.get((baseUri / "topics" / topicName.value / "records").withQueryParam("fields", "count")))
      } yield assertTrue(response.body == Right(json"2"))
    },
    test("should fail when retrieving records") {
      val topicName = TopicName("recordCountSpecTopicTwo")
      for {
        _        <- KafkaUtils.createTopic(topicName, numPartition = 1)
        _        <- KafkaUtils.produce(topicName, "k", "v")
        _        <- KafkaUtils.produce(topicName, "k", "v")
        response <- sendHttpRequest(baseUri => emptyRequest.get(baseUri / "topics" / topicName.value / "records"))
      } yield assertTrue(response.code == StatusCode.BadRequest)
    },
    test("should fail when retrieving anything but count field") {
      val topicName = TopicName("recordCountSpecTopicThree")
      for {
        _        <- KafkaUtils.createTopic(topicName, numPartition = 1)
        _        <- KafkaUtils.produce(topicName, "k", "v")
        _        <- KafkaUtils.produce(topicName, "k", "v")
        response <- sendHttpRequest(baseUri =>
                      emptyRequest.get((baseUri / "topics" / topicName.value / "records").withQueryParam("fields", "payload"))
                    )
      } yield assertTrue(response.code == StatusCode.BadRequest)
    },
  )
  private val endOffsetSpec      = suite("/offsets/end")(
    test("should return topic first offset") {
      val topicOne = TopicName("endOffsetSpecOne")
      val topicTwo = TopicName("endOffsetSpecTwo")
      for {
        _        <- KafkaUtils.createTopic(topicOne, numPartition = 1)
        _        <- KafkaUtils.createTopic(topicTwo, numPartition = 2)
        _        <- KafkaUtils.produce(topicOne, "k", "v")
        response <-
          sendHttpRequest(baseUri =>
            emptyRequest
              .get((baseUri / "offsets" / "end"))
              .body(
                json"""[{"topicName": ${topicOne.value}, "partition": 0}, {"topicName": ${topicTwo.value}, "partition": 0}, {"topicName": ${topicTwo.value}, "partition": 1}]"""
              )
          )
        //TODO: go for prettier solution
        actual    = response.body.map { json =>
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
        actual ==
          Right(json"""[{"topicName": ${topicOne.value}, "partition": 0, "offset": 1}, {"topicName": ${topicTwo.value}, "partition": 0, "offset": 0},
                {"topicName": ${topicTwo.value}, "partition": 1, "offset": 0}]""")
      )
    }
  )

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("RestEndpoint")(
      suite("not shared kafka")(
        allNameSpec,
        topicSizeSpec,
      ).provide(
        HttpClientZioBackend.layer(),
        App.serverLayer(0),
        RestEndpointsLive.layer,
        KafkaTestContainer.kafkaLayer,
        KafkaServiceLive.layer,
        TopicInfoStreamServiceLive.layer,
        TopicInfoPaginatedStreamServiceLive.layer,
        AdminClient.live,
        KafkaUtils.adminClientSettingsLayer,
        KafkaUtils.producerLayer,
      ),
      suite("shared kafka")(
        spreadSpec,
        partitionCountSpec,
        replicationFactorSpec,
        recordCountSpec,
        beginningOffsetSpec,
        endOffsetSpec,
        describeTopicSpec,
      ).provideShared(
        HttpClientZioBackend.layer(),
        App.serverLayer(0),
        RestEndpointsLive.layer,
        KafkaTestContainer.kafkaLayer,
        TopicInfoStreamServiceLive.layer,
        TopicInfoPaginatedStreamServiceLive.layer,
        KafkaServiceLive.layer,
        AdminClient.live,
        KafkaUtils.adminClientSettingsLayer,
        KafkaUtils.producerLayer,
      ),
    )

}
