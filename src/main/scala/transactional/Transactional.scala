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
            val comm       = commit.applyNonEmpty(c)
            val rollErr    = rollbackErr
            val rollCancel = rollbackCancel
            val comp       = compensate.applyNonEmpty(c)

            hooks.add(comm, rollCancel, rollErr, comp) >>= { newHooks =>
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
                val commit     = Some(finalizers(ExitCase.Succeeded))
                val rollErr    = Some((e: Throwable) => finalizers(ExitCase.Errored(e)))
                val rollCancel = Some(finalizers(ExitCase.Canceled))
                val comp       = compensate.applyNonEmpty(c)

                hooks.add(commit, rollCancel, rollErr, comp) >>= { newHooks =>
                  stack match
                    case Stack.Nil               => newHooks.onCommit(poll) as c
                    case Stack.Frame(head, tail) => loop(head(c), tail, newHooks)
                }
              }

            case Resource.Bind(source, fs) =>
              loop(Bind(FromResource(source, None), x => FromResource(fs(x), compensate)), stack, hooks)

            case Resource.Pure(a)  => loop(pure(a), stack, hooks)
            case Resource.Eval(fa) => loop(eval(fa), stack, hooks)
        
    loop(this, Stack.Nil, Hooks.empty).widen
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

  case FromResource[F[_], A](r: Resource[F, A], compensate: Option[A => F[Unit]])
    extends Transactional[F, A]

end Transactional

object Transactional:
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
    raw(fa,Some(commit),Some(rollbackErr),Some(rollbackCancel),Some(compensate))

  def eval[F[_], A](fa: F[A]): Transactional[F, A] =
    raw(fa)

  def commit[F[_]: Applicative](commit: F[Unit], compensate: F[Unit]): Transactional[F, Unit] =
    raw(Applicative[F].unit, commit = Some(_ => commit), compensate = Some(_ => compensate))

  def pure[F[_], A](a: A): Transactional[F, A] =
    Pure(a)

  def resource[F[_], A](res: Resource[F, A], compensate: A => F[Unit]): Transactional[F, A] =
    FromResource(res, Some(compensate))

  def resourceNoCompensate[F[_], A](res: Resource[F, A]): Transactional[F, A] =
    FromResource(res, None)