package chana.mq.amqp.server.store

import com.datastax.driver.core.{ BoundStatement, ResultSet, ResultSetFuture }
import scala.concurrent.CanAwait
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Success
import scala.util.Try
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit

package object cassandra {

  object resultset extends CassandraResultSetOps
  object boundstatement extends BoundStatementOps

  trait CassandraResultSetOps {
    private case class ExecutionContextExecutor(executionContext: ExecutionContext) extends java.util.concurrent.Executor {
      def execute(command: Runnable) = executionContext.execute(command)
    }

    protected class RichResultSetFuture(resultSetFuture: ResultSetFuture) extends Future[ResultSet] {
      @throws(classOf[InterruptedException])
      @throws(classOf[scala.concurrent.TimeoutException])
      def ready(atMost: Duration)(implicit permit: CanAwait): this.type = {
        resultSetFuture.get(atMost.toMillis, TimeUnit.MILLISECONDS)
        this
      }

      @throws(classOf[Exception])
      def result(atMost: Duration)(implicit permit: CanAwait): ResultSet = {
        resultSetFuture.get(atMost.toMillis, TimeUnit.MILLISECONDS)
      }

      def onComplete[U](f: Try[ResultSet] => U)(implicit executor: ExecutionContext) {
        if (resultSetFuture.isDone) {
          f(Success(resultSetFuture.getUninterruptibly))
        } else {
          resultSetFuture.addListener(new Runnable {
            def run() {
              f(Try(resultSetFuture.get))
            }
          }, ExecutionContextExecutor(executor))
        }
      }

      def isCompleted: Boolean = resultSetFuture.isDone
      def value: Option[Try[ResultSet]] = if (resultSetFuture.isDone) Some(Try(resultSetFuture.get())) else None

      def transform[S](f: Try[ResultSet] => Try[S])(implicit executor: ExecutionContext): Future[S] = {
        val p = Promise[S]
        if (resultSetFuture.isDone) {
          p.complete(f(Try(resultSetFuture.getUninterruptibly)))
        } else {
          resultSetFuture.addListener(new Runnable {
            def run() {
              p.complete(f(Try(resultSetFuture.get)))
            }
          }, ExecutionContextExecutor(executor))
        }
        p.future
      }

      def transformWith[S](f: Try[ResultSet] => Future[S])(implicit executor: ExecutionContext): Future[S] = {
        if (resultSetFuture.isDone) {
          f(Try(resultSetFuture.getUninterruptibly))
        } else {
          val p = Promise[S]
          resultSetFuture.addListener(new Runnable {
            def run() {
              p.completeWith(f(Try(resultSetFuture.get)))
            }
          }, ExecutionContextExecutor(executor))
          p.future
        }
      }
    }

    implicit def toFuture(resultSetFuture: ResultSetFuture): Future[ResultSet] = new RichResultSetFuture(resultSetFuture)
  }

  trait Binder[-A] {
    def bind(value: A, boundStatement: BoundStatement): Unit
  }

  trait BoundStatementOps {
    implicit class RichBoundStatement[A: Binder](boundStatement: BoundStatement) {
      val binder = implicitly[Binder[A]]

      def bindFrom(value: A): BoundStatement = {
        binder.bind(value, boundStatement)
        boundStatement
      }
    }
  }

}
