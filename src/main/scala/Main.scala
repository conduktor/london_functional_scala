import zio._
import zio.Console.printLine

/*
[x] list data: topic name / partition / replication / record count / topic size / spread
[] implement services to retrieve each data
[] tapir RESTful
[] Design protocol
[] Implement SSE + Command
[] Implement stateful stream
 */





object Main extends ZIOAppDefault {
  override def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
    printLine("Welcome to your first ZIO app!")
}

