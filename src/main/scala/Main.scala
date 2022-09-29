import io.circe.Encoder.AsArray.importedAsArrayEncoder
import io.circe.Encoder.AsRoot.importedAsRootEncoder
import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import io.circe._
import sttp.tapir.PublicEndpoint
import sttp.tapir.generic.auto.schemaForCaseClass
import sttp.tapir.json.circe.jsonBody
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.ztapir._
import zhttp.http.middleware.Cors.CorsConfig
import zhttp.http.{Http, HttpApp, Method, Middleware, Request, Response}
import zhttp.service.Server
import zio._

case class TopicData(name: String, sizeInByte: String, partitions: String, recordCount: String, spread: String, replicationFactor: String)



object ZioHttpServer extends ZIOAppDefault {

  val someDatas: List[TopicData] = List(
    TopicData(name = "yo", sizeInByte = "42", partitions = "3", recordCount = "43", spread = "0.8", replicationFactor = "2"),
    TopicData(name = "ya", sizeInByte = "34", partitions = "2", recordCount = "32", spread = "0.9", replicationFactor = "3"),
  )

  implicit val topicDataEncoder: Encoder[TopicData] = deriveEncoder
  implicit val topicDataDecoder: Decoder[TopicData] = deriveDecoder

  // a simple string-only endpoint
  val someTopics: PublicEndpoint[String, Unit, List[TopicData], Any] =
    endpoint.get
      .in("hello")
      .in(path[String]("name"))
      .out(jsonBody[List[TopicData]])

  private val someTopicsData: Http[Any, Throwable, Request, Response] =
    ZioHttpInterpreter().toHttp(someTopics.zServerLogic(_ => ZIO.succeed(someDatas)))

  // converting the endpoint descriptions to the Http type
  val config: CorsConfig =
    CorsConfig(
      anyOrigin = true,
      anyMethod = true,
      allowedOrigins = s => s.equals("localhost"),
      allowedMethods = Some(Set(Method.GET, Method.POST))
    )

  val app: HttpApp[Any, Throwable] = someTopicsData @@ Middleware.cors(config)

  // starting the server
  override def run =
    (for {
      _ <- ZIO.debug("starting")
      _ <- Server.start(8090, app).tapErrorCause(cause => ZIO.logErrorCause(cause))
      _ <- ZIO.debug("started")
    } yield ()).exitCode

}
