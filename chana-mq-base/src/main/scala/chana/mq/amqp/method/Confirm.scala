package chana.mq.amqp.method

import chana.mq.amqp.model.ValueReader
import java.io.DataInputStream
import java.io.IOException

/*-
 * confirm            = C:SELECT S:SELECT-OK
 */
object Confirm extends AMQClass {
  val id = 85
  val name = "confirm"

  @throws(classOf[IOException])
  def readFrom(in: DataInputStream): Method = {
    val rdr = new ArgumentsReader(new ValueReader(in))
    in.readShort() match {
      case 10  => Select(rdr.readBit())
      case 11  => SelectOk
      case mId => throw new UnknownClassOrMethodId(id, mId)
    }
  }

  final case class Select(nowait: Boolean) extends Method(10, "select") {

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeBit(this.nowait)
    }
  }

  case object SelectOk extends Method(11, "select-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
    }
  }
}
