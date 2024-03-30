package transactional

import cats.effect.*
import cats.*
import cats.effect.implicits.given
import cats.implicits.given
import scala.collection.immutable.VectorMap
import cats.effect.kernel.Unique.Token

enum Transactional[F[_], +A]:
  def flatMap[B](f: A => Transactional[F, B]): Transactional[F, B] = Bind(this, f)
  def map[B](f: A => B): Transactional[F, B]  = Bind(this, x => Pure(f(x)))

  def run[AA >: A](using F: MonadCancelThrow[F], U: Unique[F]): F[AA] = F.uncancelable: poll =>
    enum Stack[AA]:
      case Nil extends Stack[A]
      case Frame[AA, BB](head: AA => Transactional[F, BB], tail: Stack[BB]) extends Stack[AA]

    def loop[C](current: Transactional[F, C], stack: Stack[C], hooks: Hooks[F]): F[A] =
      current match
        case Transact(fc, commit, rollbackErr, rollbackCancel, compensate) =>
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
                      F.pure(c) <* newHooks.onCommit(poll)

                    case Stack.Frame(head, tail) =>
                      loop(head(c), tail, newHooks) // This is necessary, but my intuition is struggling with it.
                }}

        case Pure(c) =>
          stack match
            case Stack.Nil =>
              F.pure(c) <* hooks.onCommit(poll)

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