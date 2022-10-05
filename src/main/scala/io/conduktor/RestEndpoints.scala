package io.conduktor

import io.circe.generic.semiauto.deriveCodec
import io.circe.{Codec, Decoder, Encoder}
import io.conduktor.KafkaService.TopicName
import io.conduktor.RestEndpointsLive.TopicData
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
  implicit val errorInfoCoded = deriveCodec[ErrorInfo]

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

  implicit val topicNameCodec: Codec[TopicName] = {
    Codec
      .from(Decoder.decodeString, Encoder.encodeString)
      .iemap(str => Right(TopicName(str)))(_.value)
  }

  implicit val topicDataEncoder: Codec[TopicData] = deriveCodec

  val allTopicsName =
    endpoint.get
      .in("names")
      .errorOut(jsonBody[ErrorInfo])
      .out(jsonBody[Seq[TopicName]])
      .zServerLogic(_ => kafkaService.listTopicNames.handleError)

  //TODO: remove
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
        allowedMethods = Some(Set(Method.GET, Method.POST))
      )

    ZioHttpInterpreter().toHttp(List(allTopicsName, allInOne)) @@ Middleware
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
