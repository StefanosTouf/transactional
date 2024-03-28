import cats.effect.*
import cats.*
import cats.effect.implicits.given
import cats.implicits.given
import scala.collection.immutable.VectorMap
import cats.effect.kernel.Unique.Token
import scala.annotation.tailrec

enum Transactional[F[_], +A]:
  def fold[B](f: A => F[B])(using F: MonadCancelThrow[F], U: Unique[F]): F[B] =
    enum Stack[AA]:
      case Nil extends Stack[A]
      case Frame[AA, BB](head: AA => Transactional[F, BB], tail: Stack[BB]) extends Stack[AA]

    def loop[C](current: Transactional[F, C], stack: Stack[C], hooks: Hooks[F]): F[B] =
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
                      poll(f(c))
                        .onCancel(hooks.onCancel)
                        .onError(hooks.onError(_))
                        .<*(newHooks.onCommit)

                    case Stack.Frame(head, tail) =>
                      loop(head(c), tail, newHooks)
                }}

        case Pure(c) => F.uncancelable: poll =>
          stack match
            case Stack.Nil =>
                poll(f(c))
                  .onCancel(hooks.onCancel)
                  .onError(hooks.onError(_))
                  .<*(hooks.onCommit)

            case Stack.Frame(head, tail) =>
              loop(head(c), tail, hooks)

        case Bind(source, f) =>
          loop(source, Stack.Frame(f, stack), hooks)

    loop(this, Stack.Nil, Hooks(VectorMap(), VectorMap(), VectorMap(), VectorMap()))

  end fold

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
      , compensates     = rollbackCancels.updatedWith(token)(_ => compensate)
      )

  // Cant use traverse on VectorMap
  def onCancel: F[Unit] =
    rollbackCancels.foldLeft(F.unit):
      case (acc, (_, r)) => acc >> r 

  def onError(t: Throwable): F[Unit] =
    rollbackErrs.foldLeft(F.unit):
      case (acc, (_, r)) => acc >> r(t)

  def onCompensate: F[Unit] =
    compensates.foldLeft(F.unit):
      case (acc, (_, r)) => acc >> r

  def onCommit: F[Unit] = F.uncancelable: poll =>
    def loop(hooks: Hooks[F]): F[Unit] =
      hooks.commits.headOption match
        case None => F.unit

        case Some((token, commit)) =>
          val commited: F[Unit] =
            poll(commit)
              .onCancel(hooks.onCancel >> hooks.onCompensate)
              .onError(hooks.onError(_) >> hooks.onCompensate) // Does uncancellable work for the onError finalizer?

          // If you commit something you can no longer rollback, but you can compensate in case another commit fails
          commited >> loop:
            Hooks( hooks.commits.tail
                  , hooks.rollbackCancels.removed(token)
                  , hooks.rollbackErrs.removed(token)
                  , hooks.compensates.updatedWith(token)(_ => parent.compensates.get(token)) // Does this preserve insertion order?
                  )

    loop(parent.copy(compensates = VectorMap()))

  end onCommit

end Hooks

// Utils
extension [F[_]: Applicative, A](of: Option[A => F[Unit]])
  def applyNonEmpty(a: A): Option[F[Unit]] =
    of.map(_(a))