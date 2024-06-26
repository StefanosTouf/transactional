package transactional

import cats.effect.*
import cats.*
import cats.effect.implicits.given
import cats.implicits.given
import scala.collection.immutable.VectorMap
import cats.effect.kernel.Unique.Token

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
  ( commit        : F[Unit]
  , rollbackCancel: F[Unit]
  , rollbackErr   : Throwable => F[Unit]
  , compensate    : F[Unit]
  ): F[Hooks[F]] =
    U.unique.map: token =>
      copy(
        commits         = commits         + (token -> commit)
      , rollbackCancels = rollbackCancels + (token -> rollbackCancel)
      , rollbackErrs    = rollbackErrs    + (token -> rollbackErr)
      , compensates     = compensates     + (token -> compensate)
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

  def onCommit(poll: Poll[F]): F[Unit] =
    def loop(hooks: Hooks[F]): F[Unit] =
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

          commited >> loop(newHooks)

    loop(parent.copy(compensates = VectorMap()))

  end onCommit

  def onNonSuccess[A](fa: F[A]): F[A] =
    fa.onCancel(onCancel)
      .onError(onError(_))

end Hooks

object Hooks:
  def empty[F[_]: MonadCancelThrow: Unique]: Hooks[F] =
    Hooks(VectorMap(), VectorMap(), VectorMap(), VectorMap())