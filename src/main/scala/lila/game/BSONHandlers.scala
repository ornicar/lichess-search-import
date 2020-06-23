package lila.game

import chess.format.FEN
import chess.variant.{ Crazyhouse, Variant }
import chess.{ Black, CheckCount, Clock, Color, Mode, Status, UnmovedRooks, White }
import org.joda.time.DateTime
import reactivemongo.api.bson._
import scala.util.{ Failure, Success, Try }

import chess.Centis
import lila.db.{ BSON, ByteArray }

object BSONHandlers {

  import lila.db.ByteArray.ByteArrayBSONHandler
  import lila.db.BSON._

  implicit val StatusBSONHandler = tryHandler[Status](
    {
      case BSONInteger(v) =>
        Status(v).fold[Try[Status]](Failure(new Exception(s"No such status: $v")))(Success.apply)
    },
    x => BSONInteger(x.id)
  )

  implicit private[game] val unmovedRooksHandler = tryHandler[UnmovedRooks](
    { case bin: BSONBinary => ByteArrayBSONHandler.readTry(bin) map BinaryFormat.unmovedRooks.read },
    x => ByteArrayBSONHandler.writeTry(BinaryFormat.unmovedRooks write x).get
  )

  implicit val gameBSONHandler: BSON[Game] = new BSON[Game] {

    import Game.BSONFields._
    import Player.playerBSONHandler

    private val emptyPlayerBuilder = playerBSONHandler.read(BSONDocument())

    def reads(r: BSON.Reader): Game = {
      val winC               = r boolO winnerColor map Color.apply
      val (whiteId, blackId) = r str playerIds splitAt 4
      val uids               = r.getO[List[String]](playerUids) getOrElse Nil
      val (whiteUid, blackUid) =
        (uids.headOption.filter(_.nonEmpty), uids.lift(1).filter(_.nonEmpty))
      def player(
          field: String,
          color: Color,
          id: Player.Id,
          uid: Player.UserId
      ): Player = {
        val builder =
          r.getO[Player.Builder](field)(playerBSONHandler) | emptyPlayerBuilder
        val win = winC map (_ == color)
        builder(color)(id)(uid)(win)
      }

      val g = Game(
        id = r str id,
        whitePlayer = player(whitePlayer, White, whiteId, whiteUid),
        blackPlayer = player(blackPlayer, Black, blackId, blackUid),
        status = r.get[Status](status),
        turns = r int turns,
        startedAtTurn = r intD startedAtTurn,
        daysPerTurn = r intO daysPerTurn,
        mode = Mode(r boolD rated),
        variant = Variant(r intD variant) | chess.variant.Standard,
        createdAt = r date createdAt,
        movedAt = r.dateD(movedAt, r date createdAt),
        metadata = Metadata(
          source = r intO source flatMap Source.apply,
          tournamentId = r strO tournamentId,
          analysed = r boolD analysed
        )
      )

      val gameClock = r.getO[Color => Clock](clock)(
        clockBSONReader(
          g.createdAt,
          g.whitePlayer.berserk,
          g.blackPlayer.berserk
        )
      ) map (_(g.turnColor))

      g.copy(clock = gameClock)
    }

    def writes(w: BSON.Writer, o: Game) = ???
  }

  implicit val gameWithAnalysedBSONHandler: BSON[Game.WithAnalysed] =
    new BSON[Game.WithAnalysed] {
      def reads(r: BSON.Reader): Game.WithAnalysed = {
        Game.WithAnalysed(
          gameBSONHandler.reads(r),
          r boolD Game.BSONFields.analysed
        )
      }

      def writes(w: BSON.Writer, o: Game.WithAnalysed) = ???
    }

  private def clockHistory(
      color: Color,
      clockHistory: Option[ClockHistory],
      clock: Option[Clock],
      flagged: Option[Color]
  ) =
    for {
      clk     <- clock
      history <- clockHistory
      times = history(color)
    } yield BinaryFormat.clockHistory.writeSide(
      clk.limit,
      times,
      flagged contains color
    )

  private[game] def clockBSONReader(since: DateTime, whiteBerserk: Boolean, blackBerserk: Boolean) =
    new BSONReader[Color => Clock] {
      def readTry(bson: BSONValue): Try[Color => Clock] =
        bson match {
          case bin: BSONBinary =>
            ByteArrayBSONHandler readTry bin map { cl =>
              BinaryFormat.clock(since).read(cl, whiteBerserk, blackBerserk)
            }
          case b => lila.db.BSON.handlerBadType(b)
        }
    }
}
