package io.conduktor

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.conduktor.CirceCodec._
import io.conduktor.KafkaService.{BrokerId, Offset, Partition, PartitionInfo, RecordCount, TopicDescription, TopicName, TopicPartition, TopicSize}
import sttp.tapir.{Endpoint, Schema, Validator}
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

  implicit val topicSizeMapSchema: Schema[Map[TopicName, TopicSize]] =
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

  val recordCount =
    endpoint.get
      .in("topics")
      .in(path[TopicName]("topicName"))
      .in("records")
      .in(query[String]("fields")
        .validate(Validator.enumeration("count" :: Nil)))
      .errorOut(jsonBody[ErrorInfo])
      .out(jsonBody[RecordCount])
      .zServerLogic { case (topicName, fields) =>
        kafkaService.recordCount(topicName).handleError
      }

  val sizeTopics =
    endpoint.get
      .in("size")
      .errorOut(jsonBody[ErrorInfo])
      .out(jsonBody[Map[TopicName, TopicSize]])
      .zServerLogic { _ =>
        kafkaService.getTopicSize.handleError
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
        describeTopics,
        sizeTopics,
        beginningOffsets,
        endOffsets,
        recordCount
      )
    ) @@ Middleware
      .cors(config)
  }

}

object RestEndpointsLive {
  val layer: ZLayer[KafkaService, Nothing, RestEndpoints] = ZLayer {
    for {
      kafkaService <- ZIO.service[KafkaService]
    } yield new RestEndpointsLive(kafkaService)
  }
}
