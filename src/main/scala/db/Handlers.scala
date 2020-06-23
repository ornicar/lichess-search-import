package lila.db

import org.joda.time.DateTime
import reactivemongo.api.bson._
import reactivemongo.api.bson.exceptions.TypeDoesNotMatchException
import scala.util.{ Failure, Success, Try }
import scalaz.NonEmptyList

trait Handlers {

  implicit val BSONJodaDateTimeHandler = quickHandler[DateTime](
    { case v: BSONDateTime => new DateTime(v.value) },
    v => BSONDateTime(v.getMillis)
  )

  def quickHandler[T](read: PartialFunction[BSONValue, T], write: T => BSONValue): BSONHandler[T] =
    new BSONHandler[T] {
      def readTry(bson: BSONValue) =
        read
          .andThen(Success(_))
          .applyOrElse(
            bson,
            (b: BSONValue) => handlerBadType(b)
          )
      def writeTry(t: T) = Success(write(t))
    }

  def tryHandler[T](read: PartialFunction[BSONValue, Try[T]], write: T => BSONValue): BSONHandler[T] =
    new BSONHandler[T] {
      def readTry(bson: BSONValue) =
        read.applyOrElse(
          bson,
          (b: BSONValue) => handlerBadType(b)
        )
      def writeTry(t: T) = Success(write(t))
    }

  def handlerBadType[T](b: BSONValue): Try[T] =
    Failure(TypeDoesNotMatchException("BSONValue", b.getClass.getSimpleName))

  // implicit def bsonArrayToListHandler[T](implicit
  //     reader: BSONReader[_ <: BSONValue, T],
  //     writer: BSONWriter[T, _ <: BSONValue]
  // ): BSONHandler[BSONArray, List[T]] =
  //   new BSONHandler[BSONArray, List[T]] {
  //     def read(array: BSONArray) = readStreamList(array, reader.asInstanceOf[BSONReader[BSONValue, T]])
  //     def write(repr: List[T]) =
  //       new BSONArray(repr.map(s => scala.util.Try(writer.write(s))).to[Stream])
  //   }

  // implicit def bsonArrayToVectorHandler[T](implicit
  //     reader: BSONReader[_ <: BSONValue, T],
  //     writer: BSONWriter[T, _ <: BSONValue]
  // ): BSONHandler[BSONArray, Vector[T]] =
  //   new BSONHandler[BSONArray, Vector[T]] {
  //     def read(array: BSONArray) = readStreamVector(array, reader.asInstanceOf[BSONReader[BSONValue, T]])
  //     def write(repr: Vector[T]) =
  //       new BSONArray(repr.map(s => scala.util.Try(writer.write(s))).to[Stream])
  //   }

  // private def readStreamList[T](array: BSONArray, reader: BSONReader[BSONValue, T]): List[T] =
  //   array.stream
  //     .filter(_.isSuccess)
  //     .view
  //     .map { v =>
  //       reader.read(v.get)
  //     }
  //     .toList

  // private def readStreamVector[T](array: BSONArray, reader: BSONReader[BSONValue, T]): Vector[T] =
  //   array.stream
  //     .filter(_.isSuccess)
  //     .view
  //     .map { v =>
  //       reader.read(v.get)
  //     }
  //     .toList
}
