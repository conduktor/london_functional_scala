package io.conduktor.v2

object LanguageExtensions {
  implicit class MapOps[A, B](self: Map[A, B]) {
    def join[C, D](other: Map[A, C])(f: (B, C) => Option[D]): Map[A, D] =
      Map.from(
        self.flatMap { case (key, value) =>
          other
            .get(key)
            .map(value -> _)
            .flatMap(f.tupled)
            .map(key -> _)
        }
      )

  }
}
