package chana.mq.amqp.model

import akka.util.ByteString
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.io.UnsupportedEncodingException
import java.time.Instant

/**
 * Number of connections is limited by OS and physical resources: file descriptors available to the server
 * and RAM. Channels also consume RAM. There is is a limit of 65535 channels per connection in most clients
 * as well. See https://groups.google.com/d/msg/rabbitmq-users/E4-cXmbJzH0/6ErXGjF2K5QJ,
 * which discusses MQTT but is true for every protocol RabbitMQ supports.
 *
 * The number of exchanges is limited by RAM for RAM nodes and (in theory) disk space for disk nodes.
 * I don't think we've ever heard from anyone maxing out on the number of exchanges.
 *
 *
 * General Frame Format
 * All frames start with a 7-octet header composed of a type field (octet), a channel field (short integer) and a
 * size field (long integer):
 * 0      1         3         7                     size+7 size+8
 * +------+---------+---------+   +-------------+  +-----------+
 * | type | channel | size    |   |  payload    |  | frame-end |
 * +------+---------+---------+   +-------------+  +-----------+
 *  octet   short    long          'size' octets      octet
 *
 * AMQP defines these frame types:
 *  Type = 1, "METHOD": method frame.
 *  Type = 2, "HEADER": content header frame.
 *  Type = 3, "BODY": content body frame.
 *  Type = 4, "HEARTBEAT": heartbeat frame.
 * The channel number is 0 for all frames which are global to the connection and 1-65535 for frames that
 * refer to specific channels.
 */
object Frame {
  /** Frame type */
  val METHOD = 1
  val HEADER = 2
  val BODY = 3
  val HEARTBEAT = 8
  /** TICK frame used by ChanaMQ only - Private API */
  val TICK = 0
  /** DISCONNECT frame used by ChanaMQ only - Private API */
  val DISCONNECT = 127 // 0x7F

  val FRAME_END = 206

  /**
   * type 1
   * channel 2
   * size 4
   * end 1
   */
  val NON_BODY_SIZE = 1 + 2 + 4 + 1

  val TYPE_TAG_SIZE = 1

  // --- simple test
  def main(args: Array[String]) {
    println(HEARTBEAT_FRAME_BODY)
    println(HEARTBEAT_FRAME_BODY == ByteString(HEARTBEAT, 0, 0, 0, 0, 0, 0, FRAME_END))
    println(HEARTBEAT_FRAME_BODY == HEARTBEAT_FRAME)
  }

  val HEARTBEAT_FRAME = ByteString(HEARTBEAT, 0, 0, 0, 0, 0, 0, FRAME_END)

  /**
   * Heartbeat frames MUST have a channel number of zero. A peer that receives
   * an invalid heartbeat frame MUST raise a connection exception with reply
   * code 501 (frame error)
   */
  val HEARTBEAT_FRAME_BODY: ByteString = {
    val buf = ByteString.newBuilder
    val os = new DataOutputStream(buf.asOutputStream)
    Frame(HEARTBEAT, 0, Array()).writeTo(os)
    os.flush
    buf.result
  }

  val TICK_FRAME_BODY: ByteString = {
    val buf = ByteString.newBuilder
    val os = new DataOutputStream(buf.asOutputStream)
    Frame(TICK, 0, Array()).writeTo(os)
    os.flush
    buf.result
  }

  val DISCONNECT_FRAME_BODY: ByteString = {
    val buf = ByteString.newBuilder
    val os = new DataOutputStream(buf.asOutputStream)
    Frame(DISCONNECT, 0, Array()).writeTo(os)
    os.flush
    buf.result
  }

  @throws(classOf[IOException])
  def fromBodyFragment(channelNumber: Int, body: Array[Byte], offset: Int, length: Int): Frame = {
    val bodyOut = new ByteArrayOutputStream()
    val os = new DataOutputStream(bodyOut)
    os.write(body, offset, length)
    os.flush()
    Frame(BODY, channelNumber, bodyOut.toByteArray)
  }

  @throws(classOf[IOException])
  def readFrom(is: DataInputStream): Frame = {
    val tpe = try {
      is.readUnsignedByte()
    } catch {
      case ex: IOException =>
        return null // failed
    }

    if (tpe == 'A') {
      protocolVersionMismatch(is)
    }

    val channel = is.readUnsignedShort()
    val payloadSize = is.readInt()
    val payload = Array.ofDim[Byte](payloadSize)
    is.readFully(payload)

    val frameEndMarker = is.readUnsignedByte()
    if (frameEndMarker != FRAME_END) {
      throw new MalformedFrameException("Bad frame end marker: " + frameEndMarker)
    }

    Frame(tpe, channel, payload)
  }

  @throws(classOf[IOException])
  def protocolVersionMismatch(is: DataInputStream) {
    var x: MalformedFrameException = null

    val expectedBytes = Array[Byte]('M', 'Q', 'P')
    for (expectedByte <- expectedBytes) {
      val nextByte = is.readUnsignedByte()
      if (nextByte != expectedByte) {
        throw new MalformedFrameException("Invalid AMQP protocol header from server: expected character " +
          expectedByte + ", got " + nextByte)
      }
    }

    try {
      val signature = Array.ofDim[Int](4)

      var i = 0
      while (i < 4) {
        signature(i) = is.readUnsignedByte()
        i += 1
      }

      signature match {
        case Array(1, 1, 8, 0) =>
          x = new MalformedFrameException("AMQP protocol version mismatch; we are version " +
            AMQP.PROTOCOL.MAJOR + "-" + AMQP.PROTOCOL.MINOR + "-" + AMQP.PROTOCOL.REVISION +
            ", server is 0-8")
        case _ =>
          var sig = ""
          var i = 0
          while (i < 4) {
            if (i != 0) sig += ","
            sig += signature(i)
            i += 1
          }

          x = new MalformedFrameException("AMQP protocol version mismatch; we are version " +
            AMQP.PROTOCOL.MAJOR + "-" + AMQP.PROTOCOL.MINOR + "-" + AMQP.PROTOCOL.REVISION +
            ", server sent signature " + sig);
      }
    } catch {
      case ex: IOException =>
        x = new MalformedFrameException("Invalid AMQP protocol header from server");
    }
    throw x
  }

  private def fieldValueSize(value: Any): Long = {
    TYPE_TAG_SIZE + {
      value match {
        case x: String                      => longStrSize(x)
        case x: LongString                  => (4 + x.length)
        case x: Int                         => 4
        case x: BigDecimal                  => 5
        case x: Instant                     => 8
        case x: Map[String, Any] @unchecked => (4 + tableSize(x))
        case x: Byte                        => 1
        case x: Double                      => 8
        case x: Float                       => 4
        case x: Long                        => 8
        case x: Short                       => 2
        case x: Boolean                     => 1
        case x: Array[Byte]                 => (4 + x.length)
        case x: Array[_]                    => (4 + arraySize(x))
        case x: Seq[_]                      => (4 + arraySize(x))
        case null                           => 0
        case _ =>
          throw new IllegalArgumentException("invalid value in table")
          0
      }
    }
  }

  def tableSize(table: Map[String, Any]): Long = {
    table.foldLeft(0L) {
      case (acc, (k, v)) =>
        acc + shortStrSize(k) + fieldValueSize(v)
    }
  }

  def arraySize(values: Seq[_]): Long = {
    values.foldLeft(0L) { (acc, x) =>
      acc + fieldValueSize(x)
    }
  }

  def arraySize(values: Array[_]): Long = {
    values.foldLeft(0L) { (acc, x) =>
      acc + fieldValueSize(x)
    }
  }

  @throws(classOf[UnsupportedEncodingException])
  private def longStrSize(str: String): Int = {
    str.getBytes("utf-8").length + 4
  }

  @throws(classOf[UnsupportedEncodingException])
  private def shortStrSize(str: String): Int = {
    str.getBytes("utf-8").length + 1
  }

}
final case class Frame(tpe: Int, channel: Int, payload: Array[Byte]) {

  @throws(classOf[IOException])
  def writeTo(os: DataOutputStream) {
    os.writeByte(tpe)
    os.writeShort(channel)
    os.writeInt(payload.length)
    os.write(payload)
    os.write(Frame.FRAME_END)
  }

  def size: Int = {
    payload.length + Frame.NON_BODY_SIZE
  }

  override def toString() = {
    val sb = new StringBuilder()
    sb.append("Frame(type=").append(tpe).append(", channel=").append(channel).append(", ")
    sb.append(payload.length).append(" bytes of payload)")
    sb.toString()
  }
}

class MalformedFrameException(reason: String) extends IOException(reason)