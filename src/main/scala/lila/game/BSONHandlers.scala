package lila.game

import scala.collection.breakOut
import org.joda.time.DateTime
import reactivemongo.bson._

import chess.variant.{ Variant, Crazyhouse }
import chess.{ CheckCount, Color, Clock, White, Black, Status, Mode, UnmovedRooks }
import chess.format.FEN

import lila.db.{ BSON, ByteArray }
import chess.Centis

object BSONHandlers {

  import lila.db.ByteArray.ByteArrayBSONHandler


  implicit val StatusBSONHandler = new BSONHandler[BSONInteger, Status] {
    def read(bsonInt: BSONInteger): Status = Status(bsonInt.value) err s"No such status: ${bsonInt.value}"
    def write(x: Status) = BSONInteger(x.id)
  }

  private[game] implicit val unmovedRooksHandler = new BSONHandler[BSONBinary, UnmovedRooks] {
    def read(bin: BSONBinary): UnmovedRooks = BinaryFormat.unmovedRooks.read {
      ByteArrayBSONHandler.read(bin)
    }
    def write(x: UnmovedRooks): BSONBinary = ByteArrayBSONHandler.write {
      BinaryFormat.unmovedRooks.write(x)
    }
  }

  implicit val gameBSONHandler: BSON[Game] = new BSON[Game] {

    import Game.BSONFields._
    import Player.playerBSONHandler

    private val emptyPlayerBuilder = playerBSONHandler.read(BSONDocument())

    def reads(r: BSON.Reader): Game = {
      val winC = r boolO winnerColor map Color.apply
      val (whiteId, blackId) = r str playerIds splitAt 4
      val uids = r.getO[List[String]](playerUids) getOrElse Nil
      val (whiteUid, blackUid) = (uids.headOption.filter(_.nonEmpty), uids.lift(1).filter(_.nonEmpty))
      def player(field: String, color: Color, id: Player.Id, uid: Player.UserId): Player = {
        val builder = r.getO[Player.Builder](field)(playerBSONHandler) | emptyPlayerBuilder
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

      val gameClock = r.getO[Color => Clock](clock)(clockBSONReader(g.createdAt, g.whitePlayer.berserk, g.blackPlayer.berserk)) map (_(g.turnColor))

      g.copy(clock = gameClock)
    }

    def writes(w: BSON.Writer, o: Game) = ???
  }

  implicit val gameWithAnalysedBSONHandler: BSON[Game.WithAnalysed] = new BSON[Game.WithAnalysed] {
    def reads(r: BSON.Reader): Game.WithAnalysed = {
      Game.WithAnalysed(
        gameBSONHandler.reads(r),
        r boolD Game.BSONFields.analysed)
    }

    def writes(w: BSON.Writer, o: Game.WithAnalysed) = ???
  }

  private def clockHistory(color: Color, clockHistory: Option[ClockHistory], clock: Option[Clock], flagged: Option[Color]) =
    for {
      clk <- clock
      history <- clockHistory
      times = history(color)
    } yield BinaryFormat.clockHistory.writeSide(clk.limit, times, flagged contains color)

  private[game] def clockBSONReader(since: DateTime, whiteBerserk: Boolean, blackBerserk: Boolean) = new BSONReader[BSONBinary, Color => Clock] {
    def read(bin: BSONBinary) = BinaryFormat.clock(since).read(
      ByteArrayBSONHandler read bin, whiteBerserk, blackBerserk
    )
  }
}
