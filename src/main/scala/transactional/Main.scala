package transactional

import cats.effect.*
import cats.implicits.given

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
