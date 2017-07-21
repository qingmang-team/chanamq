package chana.mq.amqp.method

import chana.mq.amqp.model.ValueReader
import java.io.DataInputStream
import java.io.IOException

/*-
 * AMQP supports two kinds of transactions:
 * 1. Automatic transactions, in which every published message and acknowledgement is processed as a
 * stand-alone transaction.
 * 2. Server local transactions, in which the server will buffer published messages and acknowledgements
 * and commit them on demand from the client.
 *
 * The Transaction class (“tx”) gives applications access to the second type, namely server transactions. The
 * semantics of this class are:
 * 1. The application asks for server transactions in each channel where it wants these transactions (Select).
 * 2. The application does work (Publish, Ack).
 * 3. The application commits or rolls-back the work (Commit, Roll-back).
 * 4. The application does work, ad infinitum.
 *
 * Transactions cover published contents and acknowledgements, not deliveries. Thus, a rollback does not
 * requeue or redeliver any messages, and a client is entitled to acknowledge these messages in a following
 * transaction.
 *
 * tx                  = C:SELECT S:SELECT-OK
 *                     / C:COMMIT S:COMMIT-OK
 *                     / C:ROLLBACK S:ROLLBACK-OK
 */
object Tx extends AMQClass {
  val id = 90
  val name = "tx"

  @throws(classOf[IOException])
  def readFrom(in: DataInputStream): Method = {
    val rdr = new ArgumentsReader(new ValueReader(in))
    in.readShort() match {
      case 10  => Select
      case 11  => SelectOk
      case 20  => Commit
      case 21  => CommitOk
      case 30  => Rollback
      case 31  => RollbackOk
      case mId => throw new UnknownClassOrMethodId(id, mId)
    }
  }

  case object Select extends Method(10, "select") {

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
    }
  }

  case object SelectOk extends Method(11, "select-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
    }
  }

  case object Commit extends Method(20, "commit") {

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
    }
  }

  case object CommitOk extends Method(21, "commit-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
    }
  }

  case object Rollback extends Method(30, "rollback") {

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
    }
  }

  case object RollbackOk extends Method(31, "rollback-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
    }
  }
}
