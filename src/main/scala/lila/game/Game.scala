package lila.game

import scala.concurrent.duration._

import chess.Color.{ White, Black }
import chess.format.{ Uci, FEN }
import chess.opening.{ FullOpening, FullOpeningDB }
import chess.variant.{ Variant, Crazyhouse }
import chess.{ History => ChessHistory, CheckCount, Castles, Board, MoveOrDrop, Pos, Game => ChessGame, Clock, Status, Color, Mode, PositionHash, UnmovedRooks, Centis }
import org.joda.time.DateTime

import lila.common.Sequence
import lila.db.ByteArray

case class Game(
    id: String,
    whitePlayer: Player,
    blackPlayer: Player,
    status: Status,
    turns: Int, // = ply
    startedAtTurn: Int,
    clock: Option[Clock] = None,
    daysPerTurn: Option[Int],
    mode: Mode = Mode.default,
    variant: Variant = Variant.default,
    createdAt: DateTime = DateTime.now,
    movedAt: DateTime = DateTime.now,
    metadata: Metadata
) {

  val players = List(whitePlayer, blackPlayer)

  def player(color: Color): Player = color match {
    case White => whitePlayer
    case Black => blackPlayer
  }

  def player(playerId: String): Option[Player] =
    players find (_.id == playerId)

  def player(c: Color.type => Color): Player = player(c(Color))

  def isPlayerFullId(player: Player, fullId: String): Boolean =
    (fullId.size == Game.fullIdSize) && player.id == (fullId drop 8)

  def player: Player = player(turnColor)

  def playerByUserId(userId: String): Option[Player] = players.find(_.userId contains userId)

  def opponent(p: Player): Player = opponent(p.color)

  def opponent(c: Color): Player = player(!c)

  lazy val firstColor = Color(whitePlayer before blackPlayer)
  def firstPlayer = player(firstColor)
  def secondPlayer = player(!firstColor)

  def turnColor = Color((turns & 1) == 0)

  def turnOf(p: Player): Boolean = p == player
  def turnOf(c: Color): Boolean = c == turnColor

  def playedTurns = turns - startedAtTurn

  def flagged = if (status == Status.Outoftime) Some(turnColor) else None

  // we can't rely on the clock,
  // because if moretime was given,
  // elapsed time is no longer representing the game duration
  def durationSeconds: Option[Int] = (movedAt.getMillis / 1000 - createdAt.getMillis / 1000) match {
    case seconds if seconds > 60 * 60 * 12 => None // no way it lasted more than 12 hours, come on.
    case seconds => Some(seconds.toInt)
  }

  def correspondenceClock: Option[CorrespondenceClock] = daysPerTurn map { days =>
    val increment = days * 24 * 60 * 60
    val secondsLeft = (movedAt.getMillis / 1000 + increment - System.currentTimeMillis() / 1000).toInt max 0
    CorrespondenceClock(
      increment = increment,
      whiteTime = turnColor.fold(secondsLeft, increment),
      blackTime = turnColor.fold(increment, secondsLeft)
    )
  }

  def speed = chess.Speed(clock.map(_.config))

  def perfKey = PerfPicker.key(this)
  def perfType = lila.rating.PerfType(perfKey)

  def started = status >= Status.Started

  def notStarted = !started

  def aborted = status == Status.Aborted

  def playedThenAborted = aborted && bothPlayersHaveMoved

  def playable = status < Status.Aborted && !imported

  def playableEvenImported = status < Status.Aborted

  def playableBy(p: Player): Boolean = playable && turnOf(p)

  def playableBy(c: Color): Boolean = playableBy(player(c))

  def playableByAi: Boolean = playable && player.isAi

  def mobilePushable = isCorrespondence && playable && nonAi

  def alarmable = hasCorrespondenceClock && playable && nonAi

  def continuable = status != Status.Mate && status != Status.Stalemate

  def aiLevel: Option[Int] = players find (_.isAi) flatMap (_.aiLevel)

  def hasAi: Boolean = players.exists(_.isAi)
  def nonAi = !hasAi

  def aiPov: Option[Pov] = players.find(_.isAi).map(_.color) map pov

  def mapPlayers(f: Player => Player) = copy(
    whitePlayer = f(whitePlayer),
    blackPlayer = f(blackPlayer)
  )

  def boosted = rated && finished && bothPlayersHaveMoved && playedTurns < 10

  def rated = mode.rated
  def casual = !rated

  def finished = status >= Status.Mate

  def finishedOrAborted = finished || aborted

  def ratingVariant =
    if (metadata.tournamentId.isDefined && variant == chess.variant.FromPosition) chess.variant.Standard
    else variant

  def fromPosition = variant == chess.variant.FromPosition || source.contains(Source.Position)

  def imported = source contains Source.Import

  def fromPool = source contains Source.Pool
  def fromLobby = source contains Source.Lobby

  def winner = players find (_.wins)

  def loser = winner map opponent

  def winnerColor: Option[Color] = winner map (_.color)

  def winnerUserId: Option[String] = winner flatMap (_.userId)

  def loserUserId: Option[String] = loser flatMap (_.userId)

  def wonBy(c: Color): Option[Boolean] = winnerColor map (_ == c)

  def lostBy(c: Color): Option[Boolean] = winnerColor map (_ != c)

  def drawn = finished && winner.isEmpty

  def isCorrespondence = speed == chess.Speed.Correspondence

  def hasClock = clock.isDefined

  def hasCorrespondenceClock = daysPerTurn.isDefined

  def isUnlimited = !hasClock && !hasCorrespondenceClock

  def estimateClockTotalTime = clock.map(_.estimateTotalSeconds)

  def estimateTotalTime = estimateClockTotalTime orElse
    correspondenceClock.map(_.estimateTotalTime) getOrElse 1200

  def onePlayerHasMoved = playedTurns > 0
  def bothPlayersHaveMoved = playedTurns > 1

  def startColor = Color(startedAtTurn % 2 == 0)

  def playerMoves(color: Color): Int =
    if (color == startColor) (playedTurns + 1) / 2
    else playedTurns / 2

  def playerHasMoved(color: Color) = playerMoves(color) > 0

  def isBeingPlayed = !finishedOrAborted

  def olderThan(seconds: Int) = movedAt isBefore DateTime.now.minusSeconds(seconds)

  def unplayed = !bothPlayersHaveMoved && (createdAt isBefore Game.unplayedDate)

  def forecastable = started && playable && isCorrespondence && !hasAi

  def userIds = playerMaps(_.userId)

  def userRatings = playerMaps(_.rating)

  def averageUsersRating = userRatings match {
    case a :: b :: Nil => Some((a + b) / 2)
    case a :: Nil => Some((a + 1500) / 2)
    case _ => None
  }

  def source = metadata.source

  def resetTurns = copy(turns = 0, startedAtTurn = 0)

  def synthetic = id == Game.syntheticId

  private def playerMaps[A](f: Player => Option[A]): List[A] = players flatMap { f(_) }

  def pov(c: Color) = Pov(this, c)
  def whitePov = pov(White)
  def blackPov = pov(Black)
}

object Game {

  type ID = String

  case class WithAnalysed(game: Game, analysed: Boolean)

  val syntheticId = "synthetic"

  val maxPlayingRealtime = 100 // plus 200 correspondence games

  val analysableVariants: Set[Variant] = Set(
    chess.variant.Standard,
    chess.variant.Crazyhouse,
    chess.variant.Chess960,
    chess.variant.KingOfTheHill,
    chess.variant.ThreeCheck,
    chess.variant.Antichess,
    chess.variant.FromPosition,
    chess.variant.Horde,
    chess.variant.Atomic,
    chess.variant.RacingKings
  )

  val unanalysableVariants: Set[Variant] = Variant.all.toSet -- analysableVariants

  val variantsWhereWhiteIsBetter: Set[Variant] = Set(
    chess.variant.ThreeCheck,
    chess.variant.Atomic,
    chess.variant.Horde,
    chess.variant.RacingKings,
    chess.variant.Antichess
  )

  val visualisableVariants: Set[Variant] = Set(
    chess.variant.Standard,
    chess.variant.Chess960
  )

  val hordeWhitePawnsSince = new DateTime(2015, 4, 11, 10, 0)

  def isOldHorde(game: Game) =
    game.variant == chess.variant.Horde &&
      game.createdAt.isBefore(Game.hordeWhitePawnsSince)

  def allowRated(variant: Variant, clock: Clock.Config) =
    variant.standard || clock.estimateTotalTime >= Centis(3000)

  val gameIdSize = 8
  val playerIdSize = 4
  val fullIdSize = 12
  val tokenSize = 4

  val unplayedHours = 24
  def unplayedDate = DateTime.now minusHours unplayedHours

  val abandonedDays = 21
  def abandonedDate = DateTime.now minusDays abandonedDays

  val aiAbandonedHours = 6
  def aiAbandonedDate = DateTime.now minusHours aiAbandonedHours

  def takeGameId(fullId: String) = fullId take gameIdSize
  def takePlayerId(fullId: String) = fullId drop gameIdSize

  object BSONFields {

    val id = "_id"
    val whitePlayer = "p0"
    val blackPlayer = "p1"
    val playerIds = "is"
    val playerUids = "us"
    val playingUids = "pl"
    val oldPgn = "pg"
    val huffmanPgn = "hp"
    val binaryPieces = "ps"
    val status = "s"
    val turns = "t"
    val startedAtTurn = "st"
    val clock = "c"
    val checkCount = "cc"
    val daysPerTurn = "cd"
    val moveTimes = "mt"
    val whiteClockHistory = "cw"
    val blackClockHistory = "cb"
    val rated = "ra"
    val analysed = "an"
    val variant = "v"
    val crazyData = "chd"
    val createdAt = "ca"
    val movedAt = "ua" // ua = updatedAt (bc)
    val source = "so"
    val tournamentId = "tid"
    val simulId = "sid"
    val winnerColor = "w"
    val winnerId = "wid"
    val initialFen = "if"
  }
}

case class CastleLastMoveTime(
    castles: Castles,
    lastMove: Option[(Pos, Pos)],
    check: Option[Pos]
) {

  def lastMoveString = lastMove map { case (a, b) => s"$a$b" }
}

object CastleLastMoveTime {

  def init = CastleLastMoveTime(Castles.all, None, None)

  import reactivemongo.bson._
  import lila.db.ByteArray.ByteArrayBSONHandler

  private[game] implicit val castleLastMoveTimeBSONHandler = new BSONHandler[BSONBinary, CastleLastMoveTime] {
    def read(bin: BSONBinary) = BinaryFormat.castleLastMoveTime read {
      ByteArrayBSONHandler read bin
    }
    def write(clmt: CastleLastMoveTime) = ByteArrayBSONHandler write {
      BinaryFormat.castleLastMoveTime write clmt
    }
  }
}

case class ClockHistory(
    white: Vector[Centis] = Vector.empty,
    black: Vector[Centis] = Vector.empty
) {

  def update(color: Color, f: Vector[Centis] => Vector[Centis]): ClockHistory =
    color.fold(copy(white = f(white)), copy(black = f(black)))

  def record(color: Color, clock: Clock): ClockHistory =
    update(color, _ :+ clock.remainingTime(color))

  def reset(color: Color) = update(color, _ => Vector.empty)

  def apply(color: Color): Vector[Centis] = color.fold(white, black)

  def last(color: Color) = apply(color).lastOption

  def size = white.size + black.size

  // first state is of the color that moved first.
  def bothClockStates(firstMoveBy: Color): Vector[Centis] =
    Sequence.interleave(
      firstMoveBy.fold(white, black),
      firstMoveBy.fold(black, white)
    )
}
