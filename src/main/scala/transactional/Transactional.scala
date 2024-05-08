package transactional

import cats.effect.*
import cats.*
import cats.implicits.given
import transactional.Transactional.*
import cats.effect.kernel.Resource.ExitCase

enum Transactional[F[_], +A]:
  def flatMap[B](f: A => Transactional[F, B]): Transactional[F, B] =
    Bind(this, f)

  def map[B](f: A => B): Transactional[F, B] =
    Bind(this, x => Pure(f(x)))

  def run[A2 >: A](using F: MonadCancelThrow[F], U: Unique[F]): F[A2] = F.uncancelable: poll =>
    enum Stack[Current]:
      case Nil extends Stack[A]

      case Frame[Current, Next]
      ( head: Current => Transactional[F, Next]
      , tail: Stack[Next]
      ) extends Stack[Current]

    def loop[C](current: Transactional[F, C], stack: Stack[C], hooks: Hooks[F]): F[A] =
      current match
        case Transact(fc, commit, rollbackErr, rollbackCancel, compensate) =>
          hooks.onNonSuccess(poll(fc)) >>= { c =>
            hooks.add(commit(c), rollbackCancel, rollbackErr, compensate(c)) >>= { newHooks =>
              stack match
                case Stack.Nil               => newHooks.onCommit(poll) as c
                case Stack.Frame(head, tail) => loop(head(c), tail, newHooks)
          }}

        case Pure(c) =>
          stack match
            case Stack.Nil               => hooks.onCommit(poll) as c
            case Stack.Frame(head, tail) => loop(head(c), tail, hooks)

        case Bind(source, f) =>
          loop(source, Stack.Frame(f, stack), hooks)

        case Transactional.FromResource(r, compensate) =>
          r match
            case Resource.Allocate(resource) =>
              hooks.onNonSuccess(resource(poll)) >>= { case (c, finalizers) =>
                val commit     = finalizers(ExitCase.Succeeded)
                val rollErr    = (e: Throwable) => finalizers(ExitCase.Errored(e))
                val rollCancel = finalizers(ExitCase.Canceled)
                val comp       = compensate(c)

                hooks.add(commit, rollCancel, rollErr, comp) >>= { newHooks =>
                  stack match
                    case Stack.Nil               => newHooks.onCommit(poll) as c
                    case Stack.Frame(head, tail) => loop(head(c), tail, newHooks)
                }
              }

            case Resource.Bind(source, fs) =>
              val bound = Bind(FromResource(source, _ => F.unit), x => FromResource(fs(x), compensate))
              loop(bound, stack, hooks)

            case Resource.Pure(a)  => loop(pure(a), stack, hooks)
            case Resource.Eval(fa) => loop(eval(fa), stack, hooks)
        
    loop(this, Stack.Nil, Hooks.empty).widen
  end run

  case Transact[F[_], A]
  ( fa            : F[A]
  , commit        : A => F[Unit]
  , rollbackErr   : Throwable => F[Unit]
  , rollbackCancel: F[Unit]
  , compensate    : A => F[Unit]
  ) extends Transactional[F, A]

  case Bind[F[_], A, +B](source: Transactional[F, A], f: A => Transactional[F, B])
    extends Transactional[F, B]

  case Pure[F[_], +A](a: A)
    extends Transactional[F, A]

  case FromResource[F[_], A](r: Resource[F, A], compensate: A => F[Unit])
    extends Transactional[F, A]

end Transactional

object Transactional:
  def full[F[_], A]
  ( fa            : F[A]
  , commit        : A => F[Unit]
  , rollbackErr   : Throwable => F[Unit]
  , rollbackCancel: F[Unit]
  , compensate    : A => F[Unit]
  ): Transactional[F, A] =
    Transact(fa,commit,rollbackErr,rollbackCancel,compensate)

  def eval[F[_], A](fa: F[A])(using F: Applicative[F]): Transactional[F, A] =
    full(fa, _ => F.unit, _ => F.unit, F.unit, _ => F.unit)

  def commit[F[_]: Applicative](commit: F[Unit], compensate: F[Unit]): Transactional[F, Unit] =
    full(fa, _ => commit, _ => F.unit, F.unit, _ => compensate)

  def pure[F[_], A](a: A): Transactional[F, A] =
    Pure(a)

  def resource[F[_], A](res: Resource[F, A], compensate: A => F[Unit]): Transactional[F, A] =
    FromResource(res, compensate)
