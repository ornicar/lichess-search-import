package lichess

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import reactivemongo.api.ReadPreference
import reactivemongo.bson._

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import java.nio.file.Paths
import reactivemongo.akkastream.{ State, cursorProducer }

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import play.api.libs.json._
import play.api.libs.ws._
import play.api.libs.ws.ahc._

import chess.format.pgn.Pgn
import chess.variant.{ Standard, Horde, Variant }
import lichess.DB.BSONDateTimeHandler
import lila.analyse.Analysis
import lila.analyse.Analysis.analysisBSONHandler
import lila.game.BSONHandlers._
import lila.game.BSONHandlers._
import lila.game.{ Game, PgnDump, Source => S }

object Main extends App {

  override def main(args: Array[String]) {

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer(
      ActorMaterializerSettings(system)
        .withInputBuffer(
          initialSize = 32,
          maxSize = 32
        )
    )
    val wsClient = StandaloneAhcWSClient()

    val datePattern = "yyyy-MM-dd"
    val dateFormatter = DateTimeFormat forPattern datePattern
    def parseDate(str: String) = scala.util.Try(dateFormatter parseDateTime str).toOption

    val endpoint = args lift 0 getOrElse {
      sys error "Missing endpoint argument"
    }
    val search = new Search(wsClient, endpoint)

    val sinceOption: Option[Either[Unit, DateTime]] = for {
      str <- args lift 1
      res <- if (str == "reset") Some(Left(())) else parseDate(str) map Right.apply
    } yield res

    val since = sinceOption match {
      case None => sys error "Missing since date argument"
      case Some(Right(date)) =>
        println(s"Resume since $date")
        date
      case _ =>
        println("Reset game index")
        Await.result(search.putMapping, 20 seconds)
        parseDate("2011-01-01").get
    }

    /**
     * Given an operation that produces a T, returns a Future containing the result of T, unless an exception is thrown,
     * in which case the operation will be retried after _delay_ time, if there are more possible retries, which is configured through
     * the _retries_ parameter. If the operation does not succeed and there is no retries left, the resulting Future will contain the last failure.
     */
    def retry[T](op: => Future[T], delay: FiniteDuration, retries: Int): Future[T] =
      op recoverWith {
        case _ if retries > 0 =>
          akka.pattern.after(delay, system.scheduler)(retry(op, delay, retries - 1))
      }

    DB.get foreach {
      case (db, dbClose) =>

        val gameSource = db.gameColl
          .find(BSONDocument("ca" -> BSONDocument("$gt" -> since)))
          .sort(BSONDocument("ca" -> 1))
          .cursor[Game.WithAnalysed](readPreference = ReadPreference.secondaryPreferred)
          .documentSource(maxDocs = Int.MaxValue)
        // .documentSource(maxDocs = 100000)

        val tickSource =
          Source.tick(Reporter.freq, Reporter.freq, None)

        def jsonBatch(entries: Seq[(String, JsObject)]) = JsObject(entries map {
          case (id, doc) => id -> JsString(Json.stringify(doc))
        })

        gameSource
          .buffer(10000, OverflowStrategy.backpressure)
          .map(g => Some(g))
          .merge(tickSource, eagerComplete = true)
          .via(Reporter)
          .map {
            case Game.WithAnalysed(g, a) => g.id -> search.toDoc(g, a)
          }
          .grouped(1000)
          .map(jsonBatch)
          .async
          .mapAsyncUnordered(2) { batch =>
            retry(search.store(batch), 10 seconds, 20)
          }
          .runWith(Sink.ignore) andThen {
            case state =>
              search.refresh map { _ =>
                dbClose()
                wsClient.close()
                system.terminate()
              }
          }
    }
  }
}
