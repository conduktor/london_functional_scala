package io.conduktor

import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import sttp.model.sse.ServerSentEvent
import io.circe.generic.extras.Configuration
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.syntax.EncoderOps
import io.conduktor.CirceCodec._
import io.conduktor.KafkaService.{
  Offset,
  Partition,
  PartitionCount,
  PartitionInfo,
  RecordCount,
  ReplicationFactor,
  Spread,
  TopicDescription,
  TopicName,
  TopicPartition,
  TopicSize,
}
import io.conduktor.TopicInfoStreamService.Info
import io.conduktor.v2.{Input, TopicInfoPaginatedStreamService}
import org.http4s._
import org.http4s.server.Router
import org.http4s.server.websocket.WebSocketBuilder2
import sttp.capabilities.zio.ZioStreams
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.server.http4s.Http4sServerOptions
import sttp.tapir.server.http4s.ztapir.{ZHttp4sServerInterpreter, serverSentEventsBody}
import sttp.tapir.server.interceptor.cors.CORSInterceptor
import sttp.tapir.ztapir._
import zio.interop.catz._
import sttp.tapir.{CodecFormat, Schema, Validator}
import zio.{Cause, IO, Promise, Queue, Task, ZIO, ZLayer}
import zio.stream.{Stream, ZStream}

trait RestEndpoints {
  def app(webSocketBuilder: WebSocketBuilder2[Task]): HttpApp[Task]
}

class RestEndpointsLive(
  kafkaService: KafkaService,
  topicInfoStreamService: TopicInfoStreamService,
  topicInfoPaginatedStreamService: TopicInfoPaginatedStreamService,
) extends RestEndpoints {

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
              Cause.fail(throwable),
            )
            .as(ErrorInfo(throwable.getMessage))
        }

  }

  implicit val config: Configuration  = Configuration.default.withDiscriminator("type")
  implicit val infoCodec: Codec[Info] =
    deriveConfiguredCodec

  implicit val commandCodec: Codec[Input.Command] =
    deriveConfiguredCodec

  val infos = endpoint.get
    .in("streaming")
    .errorOut(jsonBody[ErrorInfo])
    .out(serverSentEventsBody)
    .zServerLogic(_ => ZIO.succeed(topicInfoStreamService.streamInfos.map(info => ServerSentEvent(data = Some(info.asJson.spaces2)))))

  implicit val infoCodecSchema: Schema[Info] = Schema.string[Info] //TODO: improve

  val paginatedInfos = endpoint.get
    .in("paginated_streaming")
    .in(query[Int]("pageSize"))
    .errorOut(jsonBody[ErrorInfo])
    .out(webSocketBody[Input.Command, CodecFormat.Json, Info, CodecFormat.Json](ZioStreams))
    .zServerLogic { pageSize =>
      ZIO.succeed({ (commandStream: Stream[Throwable, Input.Command]) =>
        ZStream.unwrap(for {
          queue      <- Queue.unbounded[Input.Command]
          inputError <- Promise.make[Throwable, Nothing]
          _          <- commandStream
                          .tap(queue.offer(_))
                          .runDrain
                          .tapError(inputError.fail)
                          .fork
        } yield topicInfoPaginatedStreamService.streamInfos(pageSize = pageSize, queue = queue).interruptWhen(inputError))
      })
    }

  val allTopicsName =
    endpoint.get
      .in("names")
      .errorOut(jsonBody[ErrorInfo])
      .out(jsonBody[Seq[TopicName]])
      .zServerLogic(_ => kafkaService.listTopicNames.handleError)

  implicit val partitionMapSchema: Schema[Map[Partition, PartitionInfo]] =
    Schema.schemaForMap(_.toString)

  implicit val topicDescriptionMapSchema: Schema[Map[TopicName, TopicDescription]] =
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
      .in(
        query[String]("fields")
          .validate(Validator.enumeration("count" :: Nil))
      )
      .errorOut(jsonBody[ErrorInfo])
      .out(jsonBody[RecordCount])
      .zServerLogic { case (topicName, _) =>
        kafkaService.recordCount(topicName).handleError
      }

  val spread =
    endpoint.get
      .in("topics")
      .in(path[TopicName]("topicName"))
      .in("spread")
      .errorOut(jsonBody[ErrorInfo])
      .out(jsonBody[Spread])
      .zServerLogic { case (topicName) =>
        kafkaService.topicSpread(topicName).handleError
      }

  val replicationFactor =
    endpoint.get
      .in("topics")
      .in(path[TopicName]("topicName"))
      .in("replicationFactor")
      .out(jsonBody[ReplicationFactor])
      .errorOut(jsonBody[ErrorInfo])
      .zServerLogic(topicName =>
        kafkaService
          .describeTopics(Seq(topicName))
          .map(_.map(result => result._2.replicationFactor).head)
          .handleError
      )

  val partitionCount =
    endpoint.get
      .in("topics")
      .in(path[TopicName]("topicName"))
      .in("partitions")
      .in(
        query[String]("fields")
          .validate(Validator.enumeration("count" :: Nil))
      )
      .errorOut(jsonBody[ErrorInfo])
      .out(jsonBody[PartitionCount])
      .zServerLogic { case (topicName, _) =>
        kafkaService
          .describeTopics(Seq(topicName))
          .map(_.map { result =>
            PartitionCount(result._2.partition.size)
          }.head)
          .handleError
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
    offset: Offset,
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

  def app(webSocketBuilder: WebSocketBuilder2[Task]) = {
    val webSocketRoute: HttpRoutes[Task] = ZHttp4sServerInterpreter(
      Http4sServerOptions
        .customiseInterceptors[Task]
        .corsInterceptor(CORSInterceptor.default[Task])
        .options
    ).fromWebSocket(
      List(
        paginatedInfos
      )
    ).toRoutes(webSocketBuilder)

    val httpRoute: HttpRoutes[Task] = ZHttp4sServerInterpreter(
      Http4sServerOptions
        .customiseInterceptors[Task]
        .corsInterceptor(CORSInterceptor.default[Task])
        .options
    ).from(
      List(
        infos,
        allTopicsName,
        describeTopics,
        sizeTopics,
        beginningOffsets,
        endOffsets,
        recordCount,
        replicationFactor,
        spread,
        partitionCount,
      )
    ).toRoutes

    Router("ws/" -> webSocketRoute, "/" -> httpRoute).orNotFound
  }

}

object RestEndpointsLive {
  val layer: ZLayer[
    KafkaService with TopicInfoStreamService with TopicInfoPaginatedStreamService,
    Nothing,
    RestEndpoints,
  ] = ZLayer {
    for {
      kafkaService                    <- ZIO.service[KafkaService]
      streamService                   <- ZIO.service[TopicInfoStreamService]
      topicInfoPaginatedStreamService <- ZIO.service[TopicInfoPaginatedStreamService]
    } yield new RestEndpointsLive(kafkaService, streamService, topicInfoPaginatedStreamService)
  }
}
