package transactional

import cats.Applicative

extension [F[_]: Applicative, A](of: Option[A => F[Unit]])
  def applyNonEmpty(a: A): Option[F[Unit]] =
    of.map(_(a))
