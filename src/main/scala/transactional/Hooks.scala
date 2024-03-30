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
