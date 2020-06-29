package lichess

import scala.concurrent.{ ExecutionContext, Future }

import play.api.libs.json._
import play.api.libs.ws._
import play.api.libs.ws.ahc._
import play.api.libs.ws.JsonBodyWritables._

import lila.game._

final class Search(wsClient: StandaloneAhcWSClient, endpoint: String)(implicit ec: ExecutionContext) {

  private object Fields {

    val status        = "s"
    val turns         = "t"
    val rated         = "r"
    val perf          = "p"
    val uids          = "u"
    val winner        = "w"
    val loser         = "o"
    val winnerColor   = "c"
    val averageRating = "a"
    val ai            = "i"
    val date          = "d"
    val duration      = "l"
    val clockInit     = "ct"
    val clockInc      = "ci"
    val analysed      = "n"
    val whiteUser     = "wu"
    val blackUser     = "bu"
    val source        = "so"
  }

  private object Date {
    import org.joda.time.format.{ DateTimeFormat, DateTimeFormatter }
    val format                       = "yyyy-MM-dd HH:mm:ss"
    val formatter: DateTimeFormatter = DateTimeFormat forPattern format
  }

  def toDoc(game: Game, analysed: Boolean): JsObject =
    NoNull {
      Json.obj(
        Fields.status -> (game.status match {
          case s if s.is(_.Timeout) => chess.Status.Resign
          case s if s.is(_.NoStart) => chess.Status.Resign
          case s                    => game.status
        }).id,
        Fields.turns         -> math.ceil(game.turns.toFloat / 2),
        Fields.rated         -> game.rated,
        Fields.perf          -> game.perfType.map(_.id),
        Fields.uids          -> Some(game.userIds.toArray).filterNot(_.isEmpty),
        Fields.winner        -> game.winner.flatMap(_.userId),
        Fields.loser         -> game.loser.flatMap(_.userId),
        Fields.winnerColor   -> game.winner.fold(3)(_.color.fold(1, 2)),
        Fields.averageRating -> game.averageUsersRating,
        Fields.ai            -> game.aiLevel,
        Fields.date          -> (Date.formatter print game.movedAt),
        Fields.duration      -> game.durationSeconds,
        Fields.clockInit     -> game.clock.map(_.limitSeconds),
        Fields.clockInc      -> game.clock.map(_.incrementSeconds),
        Fields.analysed      -> analysed,
        Fields.whiteUser     -> game.whitePlayer.userId,
        Fields.blackUser     -> game.blackPlayer.userId,
        Fields.source        -> game.source.map(_.id)
      )
    }

  private def NoNull(o: JsObject) =
    JsObject {
      o.fields collect {
        case (key, value) if value != JsNull => key -> value
      }
    }

  def putMapping: Future[Unit] = HTTP("mapping/game", Json.obj())

  def store(obj: JsObject): Future[Unit] = HTTP("store/bulk/game", obj)

  def refresh: Future[Unit] = HTTP("refresh/game", Json.obj())

  private def HTTP[D: Writes, R](url: String, data: D, read: String => R): Future[R] =
    wsClient.url(s"$endpoint/$url").post(Json toJson data) flatMap {
      case res if res.status == 200 => Future.successful(read(res.body))
      case res                      => Future.failed(new Exception(s"$url ${res.status}"))
    }
  private def HTTP(url: String, data: JsObject): Future[Unit] = HTTP(url, data, _ => ())
}
