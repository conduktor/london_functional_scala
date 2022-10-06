package io.conduktor

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.conduktor.CirceCodec._
import io.conduktor.KafkaService.{
  BrokerId,
  Offset,
  Partition,
  PartitionInfo,
  TopicDescription,
  TopicName,
  TopicPartition,
  TopicSize
}
import io.conduktor.RestEndpointsLive.TopicData
import sttp.tapir.{Endpoint, Schema}
import sttp.tapir.generic.auto.schemaForCaseClass
import sttp.tapir.json.circe.jsonBody
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.ztapir._
import zhttp.http.middleware.Cors.CorsConfig
import zhttp.http.{HttpApp, Method, Middleware}
import zio.{Cause, IO, Task, ZIO, ZLayer}

trait RestEndpoints {
  def app: HttpApp[Any, Throwable]
}

class RestEndpointsLive(kafkaService: KafkaService) extends RestEndpoints {

  case class ErrorInfo(message: String)

  implicit val errorInfoCodec: Codec[ErrorInfo] =
    deriveCodec[ErrorInfo]

  implicit class HandlerErrorWrapper[A](task: Task[A]) {
    def handleError: IO[ErrorInfo, A] =
      task
        .flatMapError { throwable =>
          ZIO
            .logErrorCause(
              throwable.getMessage,
              Cause.fail(throwable)
            )
            .as(ErrorInfo(throwable.getMessage))
        }

  }

  val allTopicsName =
    endpoint.get
      .in("names")
      .errorOut(jsonBody[ErrorInfo])
      .out(jsonBody[Seq[TopicName]])
      .zServerLogic(_ => kafkaService.listTopicNames.handleError)

  implicit val partitionMapSchema: Schema[Map[Partition, PartitionInfo]] =
    Schema.schemaForMap(_.toString)

  implicit val topicDescriptionMapSchema
      : Schema[Map[TopicName, TopicDescription]] =
    Schema.schemaForMap(_.value)

  val describeTopics =
    endpoint.get
      .in("describe")
      .in(query[List[TopicName]]("topicNames"))
      .errorOut(jsonBody[ErrorInfo])
      .out(jsonBody[Map[TopicName, TopicDescription]])
      .zServerLogic { topicNames =>
        kafkaService.describeTopics(topicNames).handleError
      }

  case class TopicSizes(
      topicName: TopicName,
      partition: Partition,
      size: TopicSize
  )

  implicit val topicSizeCodec: Codec[TopicSizes] = deriveCodec[TopicSizes]

  val sizeTopics =
    endpoint.get
      .in("size")
      .errorOut(jsonBody[ErrorInfo])
      .out(jsonBody[Seq[TopicSizes]])
      .zServerLogic { _ =>
        //TODO: confirm and remove BrokerId constant (see comment on getTopicSize)
        kafkaService
          .getTopicSize(BrokerId(1))
          .map(_.map { case (TopicPartition(topic, partition), size) =>
            TopicSizes(topic, partition, size)
          }.toList)
          .handleError
      }

  case class TopicOffsets(
      topicName: TopicName,
      partition: Partition,
      offset: Offset
  )

  implicit val topicOffsets: Codec[TopicOffsets] = deriveCodec[TopicOffsets]

  val offsetEndpoint =
    endpoint.get
      .in("offsets")

  val beginningOffsets =
    offsetEndpoint
      .in("begin")
      .in(jsonBody[Seq[TopicPartition]])
      .errorOut(jsonBody[ErrorInfo])
      .out(jsonBody[Seq[TopicOffsets]])
      .zServerLogic { topicPartitions =>
        kafkaService
          .beginningOffsets(topicPartitions)
          .map(_.map { case (TopicPartition(topic, partition), offset) =>
            TopicOffsets(topic, partition, offset)
          }.toList)
          .handleError
      }

  val endOffsets =
    offsetEndpoint
      .in("end")
      .in(jsonBody[Seq[TopicPartition]])
      .errorOut(jsonBody[ErrorInfo])
      .out(jsonBody[Seq[TopicOffsets]])
      .zServerLogic { topicPartitions =>
        kafkaService
          .endOffsets(topicPartitions)
          .map(_.map { case (TopicPartition(topic, partition), offset) =>
            TopicOffsets(topic, partition, offset)
          }.toList)
          .handleError
      }

  //TODO: remove
  implicit val topicDataEncoder: Codec[TopicData] = deriveCodec
  val allInOne =
    endpoint.get
      .in("all")
      .out(jsonBody[Seq[TopicData]])
      .zServerLogic(_ =>
        ZIO.succeed(
          List(
            TopicData(
              name = "yo",
              sizeInByte = "42",
              partitions = "3",
              recordCount = "43",
              spread = "0.8",
              replicationFactor = "2"
            ),
            TopicData(
              name = "ya",
              sizeInByte = "34",
              partitions = "2",
              recordCount = "32",
              spread = "0.9",
              replicationFactor = "3"
            )
          )
        )
      )

  val app: HttpApp[Any, Throwable] = {
    val config: CorsConfig =
      CorsConfig(
        anyOrigin = true,
        anyMethod = true,
        allowedOrigins = s => s.equals("localhost"),
        allowedMethods = Some(Set(Method.GET, Method.POST, Method.PUT))
      )

    ZioHttpInterpreter().toHttp(
      List(
        allTopicsName,
        allInOne,
        describeTopics,
        sizeTopics,
        beginningOffsets,
        endOffsets
      )
    ) @@ Middleware
      .cors(config)
  }

}

object RestEndpointsLive {
  case class TopicData(
      name: String,
      sizeInByte: String,
      partitions: String,
      recordCount: String,
      spread: String,
      replicationFactor: String
  )

  val layer: ZLayer[KafkaService, Nothing, RestEndpoints] = ZLayer {
    for {
      kafkaService <- ZIO.service[KafkaService]
    } yield new RestEndpointsLive(kafkaService)
  }
}
