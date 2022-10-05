package io.conduktor

import com.dimafeng.testcontainers.KafkaContainer
import zio.{ZIO, ZLayer}

object KafkaTestContainer {
  val kafkaLayer: ZLayer[Any, Nothing, KafkaContainer] =
    ZLayer.scoped {
      ZIO.acquireRelease(ZIO.attemptBlocking {
        val container = KafkaContainer()
        container.start()
        container
      }.orDie)(kafka => ZIO.attemptBlocking(kafka.stop()).orDie)
    }
}
