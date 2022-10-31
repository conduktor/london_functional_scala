package io.conduktor.v2

import cats.Monoid

sealed trait Datapoint[+T]
object Datapoint {
  case object Unknown            extends Datapoint[Nothing]
  case object Loading            extends Datapoint[Nothing]
  case class Loaded[T](value: T) extends Datapoint[T]

  implicit class DatapointOps[T](self: Datapoint[T]) {
    def isComplete = self match {
      case Datapoint.Unknown   => false
      case Datapoint.Loading   => false
      case Datapoint.Loaded(_) => true
    }

    def whenLoaded[F[_], U](f: T => F[U])(implicit monoid: Monoid[F[U]]): F[U] =
      self match {
        case Datapoint.Unknown       => monoid.empty
        case Datapoint.Loading       => monoid.empty
        case Datapoint.Loaded(value) => f(value)
      }
  }

  implicit class DatapointMapOps[A](self: Map[A, Datapoint[?]]) {
    def isComplete: Boolean =
      self.forall {
        case (_, Datapoint.Loaded(_)) => true
        case _                        => false
      }

    def nextUnknownChunk(chunkSize: Int): Iterator[A] =
      self.view
        .collect { case (name, Datapoint.Unknown) => name }
        .grouped(chunkSize)
        .take(1)
        .flatten
  }
}
