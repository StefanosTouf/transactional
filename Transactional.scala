import cats.effect.*
import cats.*
import cats.effect.implicits.given
import cats.implicits.given
import scala.collection.immutable.VectorMap
import cats.effect.kernel.Unique.Token
import cats.effect.std.Console
import scala.annotation.tailrec

enum Transactional[F[_], +A]:
  def flatMap[B](f: A => Transactional[F, B]): Transactional[F, B] = Bind(this, f)
  def map[B](f: A => B): Transactional[F, B]  = Bind(this, x => Pure(f(x)))

  def run[AA >: A](using F: MonadCancelThrow[F], U: Unique[F], C: Console[F]): F[AA] =
    enum Stack[AA]:
      case Nil extends Stack[A]
      case Frame[AA, BB](head: AA => Transactional[F, BB], tail: Stack[BB]) extends Stack[AA]

    def loop[C](current: Transactional[F, C], stack: Stack[C], hooks: Hooks[F]): F[A] =
      current match
        case Transact(fc, commit, rollbackErr, rollbackCancel, compensate) =>
          F.uncancelable: poll =>
            val withRollbacks =
              poll(fc)
                .onCancel(hooks.onCancel)
                .onError(hooks.onError(_))

            withRollbacks.flatMap { c =>
              hooks
                .add(commit.applyNonEmpty(c), rollbackCancel, rollbackErr, compensate.applyNonEmpty(c))
                .flatMap { newHooks =>
                  stack match
                    case Stack.Nil =>
                      F.pure(c) <* poll(newHooks.onCommit)

                    case Stack.Frame(head, tail) =>
                      poll(loop(head(c), tail, newHooks)) // This is necessary, but my intuition is struggling with it.
                }}

        case Pure(c) =>
          stack match
            case Stack.Nil =>
              F.pure(c) <* hooks.onCommit

            case Stack.Frame(head, tail) =>
              loop(head(c), tail, hooks)

        case Bind(source, f) =>
          loop(source, Stack.Frame(f, stack), hooks)

    loop(this, Stack.Nil, Hooks(VectorMap(), VectorMap(), VectorMap(), VectorMap()))
      .widen

  end run

  case Transact[F[_], A]
  ( fa            : F[A]
  , commit        : Option[A => F[Unit]]
  , rollbackErr   : Option[Throwable => F[Unit]]
  , rollbackCancel: Option[F[Unit]]
  , compensate    : Option[A => F[Unit]]
  ) extends Transactional[F, A]

  case Bind[F[_], A, +B](source: Transactional[F, A], f: A => Transactional[F, B])
    extends Transactional[F, B]

  case Pure[F[_], +A](a: A)
    extends Transactional[F, A]

end Transactional

object Transactional {
  def raw[F[_], A]
  ( fa            : F[A]
  , commit        : Option[A => F[Unit]] = None
  , rollbackErr   : Option[Throwable => F[Unit]] = None
  , rollbackCancel: Option[F[Unit]] = None
  , compensate    : Option[A => F[Unit]] = None
  ): Transactional[F, A] =
    Transact(fa,commit,rollbackErr,rollbackCancel, compensate)

  def full[F[_], A]
  ( fa            : F[A]
  , commit        : A => F[Unit]
  , rollbackErr   : Throwable => F[Unit]
  , rollbackCancel: F[Unit]
  , compensate    : A => F[Unit]
  ): Transactional[F, A] =
    Transact(fa,Some(commit),Some(rollbackErr),Some(rollbackCancel), Some(compensate))

  def eval[F[_], A](fa: F[A]): Transactional[F, A] = raw(fa)

  def commit[F[_]: Applicative](commit: F[Unit], compensate: F[Unit]): Transactional[F, Unit] =
    raw(Applicative[F].unit, commit = Some(_ => commit), compensate = Some(_ => compensate))
}
/*
  Using VectorMaps here for a Map that preserves insertion order
  You can probably optimise this by thinking a bit more about it and using different data structures
*/
case class Hooks[F[_]]
( commits        : VectorMap[Token, F[Unit]]
, rollbackCancels: VectorMap[Token, F[Unit]]
, rollbackErrs   : VectorMap[Token, Throwable => F[Unit]]
, compensates    : VectorMap[Token, F[Unit]]
)(using F : MonadCancelThrow[F]
,       U : Unique[F]
 ):
  parent =>

  def add
  ( commit        : Option[F[Unit]] = None
  , rollbackCancel: Option[F[Unit]] = None
  , rollbackErr   : Option[Throwable => F[Unit]] = None
  , compensate    : Option[F[Unit]] = None
  ): F[Hooks[F]] =
    U.unique.map: token =>
      // Do all these preserve insertion order?
      copy(
        commits         = commits.updatedWith(token)(_ => commit)
      , rollbackCancels = rollbackCancels.updatedWith(token)(_ => rollbackCancel)
      , rollbackErrs    = rollbackErrs.updatedWith(token)(_ => rollbackErr)
      , compensates     = compensates.updatedWith(token)(_ => compensate)
      )

  // Cant use traverse on VectorMap
  def onCancel: F[Unit] =
    rollbackCancels.foldRight(F.unit):
      case ((_, r), acc) => acc >> r 

  def onError(t: Throwable): F[Unit] =
    rollbackErrs.foldRight(F.unit):
      case ((_, r), acc) => acc >> r(t)

  def onCompensate: F[Unit] =
    compensates.foldRight(F.unit):
      case ((_, r), acc) => acc >> r

  def onCommit: F[Unit] =
    def loop(hooks: Hooks[F]): F[Unit] = F.uncancelable: poll =>
      hooks.commits.headOption match
        case None => F.unit

        case Some((token, commit)) =>
          val commited: F[Unit] =
            poll(commit)
              .onCancel(hooks.onCancel >> hooks.onCompensate)
              .onError(hooks.onError(_) >> hooks.onCompensate) // Does uncancellable work for the onError finalizer?

          // If you commit something you can no longer rollback, but you can compensate in case another commit fails
          val newHooks: Hooks[F] =
            Hooks( hooks.commits.tail
                 , hooks.rollbackCancels.removed(token)
                 , hooks.rollbackErrs.removed(token)
                 , hooks.compensates.updatedWith(token)(_ => parent.compensates.get(token)) // Does this preserve insertion order?
                 )

          commited >> poll(loop(newHooks))

    loop(parent.copy(compensates = VectorMap()))

  end onCommit

end Hooks

// Utils
extension [F[_]: Applicative, A](of: Option[A => F[Unit]])
  def applyNonEmpty(a: A): Option[F[Unit]] =
    of.map(_(a))

object Main extends IOApp.Simple:
  import scala.concurrent.duration.DurationInt

  def sleep: IO[Unit] = IO.sleep(500.millis)

  def commit(id: Int) = IO.println(s"Commit $id")
  def rollbackErr(id: Int) = (t: Throwable) => IO.println(s"RollbackErr $id $t")
  def rollbackCancel(id: Int) = IO.println(s"RollbackCancel $id")
  def compensate(id: Int) = IO.println(s"Compensate $id")

  def action(id: Int): IO[Int] = IO.println(id) as id

  def err: IO[Nothing] = IO.raiseError(RuntimeException("Fail!"))

  val tx1: Transactional[IO, Int] =
    Transactional.full(sleep >> action(1), x => sleep >> commit(x), rollbackErr(1), rollbackCancel(1), compensate)
  
  val tx2: Transactional[IO, Int] =
    Transactional.full(sleep >> action(2), x => sleep >> commit(x), rollbackErr(2), rollbackCancel(2), compensate)
  
  val tx3: Transactional[IO, Int] =
    Transactional.full(sleep >> action(3), x => err >> commit(x), rollbackErr(3), rollbackCancel(3), compensate)

  val tx: Transactional[IO, Int] =
    for
      x <- tx1
      y <- tx2
      z <- tx3
    yield x + y + z

  override def run: IO[Unit] =
    IO.race(tx.run >>= IO.println, IO.sleep(3000.millis) >> IO.println("cancel")).void