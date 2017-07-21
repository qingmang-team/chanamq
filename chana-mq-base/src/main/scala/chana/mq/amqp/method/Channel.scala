package chana.mq.amqp.method

import chana.mq.amqp.model.ValueReader
import chana.mq.amqp.model.LongString
import java.io.DataInputStream
import java.io.IOException

/*-
 * AMQP is a multi-channelled protocol. Channels provide a way to multiplex a heavyweight TCP/IP
 * connection into several light weight connections. This makes the protocol more “firewall friendly” since
 * port usage is predictable. It also means that traffic shaping and other network QoS features can be easily
 * employed.
 *
 * Channels are independent of each other and can perform different functions simultaneously with other
 * channels, the available bandwidth being shared between the concurrent activities.
 *
 * It is expected and encouraged that multi-threaded client applications may often use a “channel-per-thread”
 * model as a programming convenience. However, opening several connections to one or more AMQP
 * servers from a single client is also entirely acceptable. The channel life-cycle is this:
 * 1. The client opens a new channel (Open).
 * 2. The server confirms that the new channel is ready (Open-Ok).
 * 3. The client and server use the channel as desired.
 * 4. One peer (client or server) closes the channel (Close).
 * 5. The other peer hand-shakes the channel close (Close-Ok).
 *
 * channel             = open-channel *use-channel close-channel
 * open-channel        = C:OPEN S:OPEN-OK
 * use-channel         = C:FLOW S:FLOW-OK
 *                     / S:FLOW C:FLOW-OK
 *                     / functional-class
 * close-channel       = C:CLOSE S:CLOSE-OK
 *                     / S:CLOSE C:CLOSE-OK
 */
object Channel extends AMQClass {
  val id = 20
  val name = "channel"

  @throws(classOf[IOException])
  def readFrom(in: DataInputStream): Method = {
    val rdr = new ArgumentsReader(new ValueReader(in))
    in.readShort() match {
      case 10  => Open(rdr.readShortstr())
      case 11  => OpenOk(rdr.readLongstr())
      case 20  => Flow(rdr.readBit())
      case 21  => FlowOk(rdr.readBit())
      case 40  => Close(rdr.readShort(), rdr.readShortstr(), rdr.readShort(), rdr.readShort())
      case 41  => CloseOk
      case mId => throw new UnknownClassOrMethodId(id, mId)
    }
  }

  final case class Open(reserved_1: String) extends Method(10, "open") {
    assert(reserved_1 != null, nonNull("reserved_1"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShortstr(this.reserved_1)
    }
  }

  final case class OpenOk(channelId: LongString) extends Method(11, "open-ok") {
    assert(channelId != null, nonNull("channelId"))

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeLongstr(this.channelId)
    }
  }

  final case class Flow(active: Boolean) extends Method(20, "flow") {

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeBit(this.active)
    }
  }

  final case class FlowOk(active: Boolean) extends Method(21, "flow-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeBit(this.active)
    }
  }

  final case class Close(replyCode: Int, replyText: String, causeClassId: Int, causeMethodId: Int) extends Method(40, "close") {
    assert(replyText != null, nonNull("replyText"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShort(this.replyCode)
      writer.writeShortstr(this.replyText)
      writer.writeShort(this.causeClassId)
      writer.writeShort(this.causeMethodId)
    }
  }

  case object CloseOk extends Method(41, "close-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
    }
  }
}