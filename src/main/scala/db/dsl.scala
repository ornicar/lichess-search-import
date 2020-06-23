// Copyright (C) 2014 Fehmi Can Saglam (@fehmicans) and contributors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lila.db

import ornicar.scalalib.Zero
import reactivemongo.api._
import reactivemongo.api.collections.GenericQueryBuilder
import reactivemongo.api.bson._

trait dsl extends LowPriorityDsl {

  type Coll = reactivemongo.api.bson.collection.BSONCollection
  type Bdoc = BSONDocument
  type Barr = BSONArray

  implicit val LilaBSONDocumentZero: Zero[BSONDocument] =
    Zero.instance($doc())

  implicit def bsonDocumentToPretty(document: BSONDocument): String = {
    BSONDocument.pretty(document)
  }

  //**********************************************************************************************//
  // Helpers
  def $empty: BSONDocument = BSONDocument.empty

  def $doc(elements: ElementProducer*): Bdoc = BSONDocument.strict(elements: _*)

  def $doc(elements: Iterable[(String, BSONValue)]): Bdoc = BSONDocument.strict(elements)

  def $arr(elements: Producer[BSONValue]*): BSONArray = {
    BSONArray(elements: _*)
  }

  def $id[T: BSONWriter](id: T): Bdoc = $doc("_id" -> id)

  def $inIds[T: BSONWriter](ids: Iterable[T]): Bdoc =
    $id($doc("$in" -> ids))

  def $boolean(b: Boolean) = BSONBoolean(b)
  def $string(s: String)   = BSONString(s)
  def $int(i: Int)         = BSONInteger(i)

  // End of Helpers
  //**********************************************************************************************//

  //**********************************************************************************************//
  // Top Level Logical Operators
  def $or(expressions: BSONDocument*): BSONDocument = {
    $doc("$or" -> expressions)
  }

  def $and(expressions: BSONDocument*): BSONDocument = {
    $doc("$and" -> expressions)
  }

  def $nor(expressions: BSONDocument*): BSONDocument = {
    $doc("$nor" -> expressions)
  }
  // End of Top Level Logical Operators
  //**********************************************************************************************//

  //**********************************************************************************************//
  // Top Level Evaluation Operators
  def $text(search: String): BSONDocument = {
    $doc("$text" -> $doc("$search" -> search))
  }

  def $text(search: String, language: String): BSONDocument = {
    $doc("$text" -> $doc("$search" -> search, "$language" -> language))
  }

  def $where(expression: String): BSONDocument = {
    $doc("$where" -> expression)
  }
  // End of Top Level Evaluation Operators
  //**********************************************************************************************//

  // Helpers
  def $eq[T: BSONWriter](value: T) = $doc("$eq" -> value)

  def $gt[T: BSONWriter](value: T) = $doc("$gt" -> value)

  /** Matches values that are greater than or equal to the value specified in the query. */
  def $gte[T: BSONWriter](value: T) = $doc("$gte" -> value)

  /** Matches any of the values that exist in an array specified in the query. */
  def $in[T: BSONWriter](values: T*) = $doc("$in" -> values)

  /** Matches values that are less than the value specified in the query. */
  def $lt[T: BSONWriter](value: T) = $doc("$lt" -> value)

  /** Matches values that are less than or equal to the value specified in the query. */
  def $lte[T: BSONWriter](value: T) = $doc("$lte" -> value)

  /** Matches all values that are not equal to the value specified in the query. */
  def $ne[T: BSONWriter](value: T) = $doc("$ne" -> value)

  /** Matches values that do not exist in an array specified to the query. */
  def $nin[T: BSONWriter](values: T*) = $doc("$nin" -> values)

  def $exists(value: Boolean) = $doc("$exists" -> value)

  trait CurrentDateValueProducer[T] {
    def produce: BSONValue
  }

  implicit class BooleanCurrentDateValueProducer(value: Boolean) extends CurrentDateValueProducer[Boolean] {
    def produce: BSONValue = BSONBoolean(value)
  }

  implicit class StringCurrentDateValueProducer(value: String) extends CurrentDateValueProducer[String] {
    def isValid: Boolean = Seq("date", "timestamp") contains value

    def produce: BSONValue = {
      if (!isValid)
        throw new IllegalArgumentException(value)

      $doc("$type" -> value)
    }
  }

  /**
    * Represents the inital state of the expression which has only the name of the field.
    * It does not know the value of the expression.
    */
  trait ElementBuilder {
    def field: String
    def append(value: BSONDocument): BSONDocument = value
  }

  /** Represents the state of an expression which has a field and a value */
  trait Expression[V] extends ElementBuilder {
    def value: V
    def toBdoc(implicit writer: BSONWriter[V]) = toBSONDocument(this)
  }

  /*
   * This type of expressions cannot be cascaded. Examples:
   *
   * {{{
   * "price" $eq 10
   * "price" $ne 1000
   * "size" $in ("S", "M", "L")
   * "size" $nin ("S", "XXL")
   * }}}
   *
   */
  case class SimpleExpression[V <: BSONValue](field: String, value: V) extends Expression[V]

  /**
    * Expressions of this type can be cascaded. Examples:
    *
    * {{{
    *  "age" $gt 50 $lt 60
    *  "age" $gte 50 $lte 60
    * }}}
    */
  case class CompositeExpression(field: String, value: BSONDocument)
      extends Expression[BSONDocument]
      with ComparisonOperators {
    override def append(value: BSONDocument): BSONDocument = {
      this.value ++ value
    }
  }

  /** MongoDB comparison operators. */
  trait ComparisonOperators { self: ElementBuilder =>

//     def $eq[T](value: T)(implicit writer: BSONWriter[T, _ <: BSONValue]): SimpleExpression[BSONValue] = {
//       SimpleExpression(field, writer.write(value))
//     }

//     /** Matches values that are greater than the value specified in the query. */
//     def $gt[T](value: T)(implicit writer: BSONWriter[T, _ <: BSONValue]): CompositeExpression = {
//       CompositeExpression(field, append($doc("$gt" -> value)))
//     }

//     /** Matches values that are greater than or equal to the value specified in the query. */
//     def $gte[T](value: T)(implicit writer: BSONWriter[T, _ <: BSONValue]): CompositeExpression = {
//       CompositeExpression(field, append($doc("$gte" -> value)))
//     }

//     /** Matches any of the values that exist in an array specified in the query. */
//     def $in[T](
//         values: Iterable[T]
//     )(implicit writer: BSONWriter[T, _ <: BSONValue]): SimpleExpression[BSONDocument] = {
//       SimpleExpression(field, $doc("$in" -> values))
//     }

//     /** Matches values that are less than the value specified in the query. */
//     def $lt[T](value: T)(implicit writer: BSONWriter[T, _ <: BSONValue]): CompositeExpression = {
//       CompositeExpression(field, append($doc("$lt" -> value)))
//     }

//     /** Matches values that are less than or equal to the value specified in the query. */
//     def $lte[T](value: T)(implicit writer: BSONWriter[T, _ <: BSONValue]): CompositeExpression = {
//       CompositeExpression(field, append($doc("$lte" -> value)))
//     }

//     /** Matches all values that are not equal to the value specified in the query. */
//     def $ne[T](value: T)(implicit writer: BSONWriter[T, _ <: BSONValue]): SimpleExpression[BSONDocument] = {
//       SimpleExpression(field, $doc("$ne" -> value))
//     }

//     /** Matches values that do not exist in an array specified to the query. */
//     def $nin[T](
//         values: Iterable[T]
//     )(implicit writer: BSONWriter[T, _ <: BSONValue]): SimpleExpression[BSONDocument] = {
//       SimpleExpression(field, $doc("$nin" -> values))
//     }

  }

  trait LogicalOperators { self: ElementBuilder =>
    def $not(f: (String => Expression[BSONDocument])): SimpleExpression[BSONDocument] = {
      val expression = f(field)
      SimpleExpression(field, $doc("$not" -> expression.value))
    }
  }

  trait ElementOperators { self: ElementBuilder =>
    def $exists(exists: Boolean): SimpleExpression[BSONDocument] = {
      SimpleExpression(field, $doc("$exists" -> exists))
    }
  }

  trait EvaluationOperators { self: ElementBuilder =>
    def $mod(divisor: Int, remainder: Int): SimpleExpression[BSONDocument] = {
      SimpleExpression(field, $doc("$mod" -> BSONArray(divisor, remainder)))
    }

    def $regex(value: String, options: String): SimpleExpression[BSONRegex] = {
      SimpleExpression(field, BSONRegex(value, options))
    }
  }

  trait ArrayOperators { self: ElementBuilder =>
    def $all[T: BSONWriter](values: Seq[T]): SimpleExpression[Bdoc] = {
      SimpleExpression(field, $doc("$all" -> values))
    }

    def $elemMatch(query: ElementProducer*): SimpleExpression[Bdoc] = {
      SimpleExpression(field, $doc("$elemMatch" -> $doc(query: _*)))
    }

    def $size(s: Int): SimpleExpression[Bdoc] = {
      SimpleExpression(field, $doc("$size" -> s))
    }
  }

  object $sort {

    def asc(field: String)  = $doc(field -> 1)
    def desc(field: String) = $doc(field -> -1)

    val naturalAsc   = asc("$natural")
    val naturalDesc  = desc("$natural")
    val naturalOrder = naturalDesc

    val createdAsc  = asc("createdAt")
    val createdDesc = desc("createdAt")
    val updatedDesc = desc("updatedAt")
  }

  implicit class ElementBuilderLike(val field: String)
      extends ElementBuilder
      with ComparisonOperators
      with ElementOperators
      with EvaluationOperators
      with LogicalOperators
      with ArrayOperators

  implicit def toBSONDocument[V: BSONWriter](expression: Expression[V]): Bdoc =
    $doc(expression.field -> expression.value)
}

sealed trait LowPriorityDsl { self: dsl =>
  // Priority lower than toBSONDocument
  implicit def toBSONElement[V <: BSONValue](
      expression: Expression[V]
  )(implicit writer: BSONWriter[V]): Producer[BSONElement] = {
    BSONElement(expression.field, expression.value)
  }
}

object dsl extends dsl with Handlers
