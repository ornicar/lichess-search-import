package lichess

import com.typesafe.config.ConfigFactory
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Failure, Try }

import reactivemongo.api._
import reactivemongo.api.bson._
import reactivemongo.api.bson.collection.BSONCollection
import reactivemongo.api.bson.exceptions.TypeDoesNotMatchException

import org.joda.time._

final class DB(val gameColl: BSONCollection, val analysisColl: BSONCollection, val userColl: BSONCollection) {

  private val userProj = Some(BSONDocument("username" -> true, "title" -> true))
  implicit private val lightUserBSONReader = new BSONDocumentReader[LightUser] {
    def readDocument(doc: BSONDocument) =
      Try {
        LightUser(
          id = doc.getAsOpt[String]("_id").get,
          name = doc.getAsOpt[String]("username").get,
          title = doc.getAsOpt[String]("title")
        )
      }
  }

  def users(g: lila.game.Game): Future[Users] =
    userColl
      .find(BSONDocument("_id" -> BSONDocument("$in" -> g.userIds)), userProj)
      .cursor[LightUser]()
      .collect[List](Int.MaxValue, Cursor.ContOnError())
      .map { users =>
        def of(p: lila.game.Player) =
          p.userId.fold(LightUser("?", "?")) { uid =>
            users.find(_.id == uid) getOrElse LightUser(uid, uid)
          }
        Users(of(g.whitePlayer), of(g.blackPlayer))
      }
}

object DB {

  private val config = ConfigFactory.load()
  private val dbUri  = config.getString("db.uri")

  val dbName   = "lichess"
  val collName = "game5"

  val driver = new reactivemongo.api.AsyncDriver

  def get: Future[(DB, () => Unit)] =
    for {
      uri  <- MongoConnection.fromString(dbUri)
      conn <- driver.connect(uri)
      db   <- conn.database(dbName)
    } yield (
      new DB(
        gameColl = db collection "game5",
        analysisColl = db collection "analysis2",
        userColl = db collection "user4"
      ),
      () => driver.close()
    )

  implicit object BSONDateTimeHandler extends BSONHandler[DateTime] {
    def readTry(time: BSONValue) =
      time match {
        case t: BSONDateTime => Try(new DateTime(t.value, DateTimeZone.UTC))
        case b               => Failure(TypeDoesNotMatchException("BSONDateTime", b.getClass.getSimpleName))
      }
    def writeTry(jdtime: DateTime) = Try(BSONDateTime(jdtime.getMillis))
  }

  def debug(v: BSONValue): String =
    v match {
      case d: BSONDocument => debugDoc(d)
      case d: BSONArray    => debugArr(d)
      case BSONString(x)   => x
      case BSONInteger(x)  => x.toString
      case BSONDouble(x)   => x.toString
      case BSONBoolean(x)  => x.toString
      case v               => v.toString
    }
  def debugArr(doc: BSONArray): String = doc.values.toList.map(debug).mkString("[", ", ", "]")
  def debugDoc(doc: BSONDocument): String =
    (doc.elements.toList map {
      case BSONElement(k, v) => s"$k: ${debug(v)}"
    }).mkString("{", ", ", "}")
}
