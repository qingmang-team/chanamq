package chana.mq.amqp.method

import chana.mq.amqp.model.ValueReader
import chana.mq.amqp.model.LongString
import java.io.DataInputStream
import java.io.IOException

/*-
 * AMQP is a connected protocol. The connection is designed to be long-lasting, and can carry multiple
 * channels. The connection life-cycle is this:
 * # The client opens a TCP/IP connection to the server and sends a protocol header. This is the only data
 * the client sends that is not formatted as a method.
 * # The server responds with its protocol version and other properties, including a list of the security
 * mechanisms that it supports (the Start method).
 * # The client selects a security mechanism (Start-Ok).
 * # The server starts the authentication process, which uses the SASL challenge-response model. It sends
 * the client a challenge (Secure).
 * # The client sends an authentication response (Secure-Ok). For example using the "plain" mechanism,
 * the response consist of a login name and password.
 * # The server repeats the challenge (Secure) or moves to negotiation, sending a set of parameters such as
 * maximum frame size (Tune).
 * # The client accepts or lowers these parameters (Tune-Ok).
 * # The client formally opens the connection and selects a virtual host (Open).
 * # The server confirms that the virtual host is a valid choice (Open-Ok).
 * # The client now uses the connection as desired.
 * # One peer (client or server) ends the connection (Close).
 * # The other peer hand-shakes the connection end (Close-Ok).
 * # The server and the client close their socket connection.
 *
 * There is no hand-shaking for errors on connections that are not fully open. Following successful protocol
 * header negotiation, which is defined in detail later, and prior to sending or receiving Open or Open-Ok, a
 * peer that detects an error MUST close the socket without sending any further data.
 *
 *
 * connection          = open-connection *use-connection close-connection
 * open-connection     = C:protocol-header
 *                       S:START C:START-OK
 *                       *challenge
 *                       S:TUNE C:TUNE-OK
 *                       C:OPEN S:OPEN-OK
 * challenge           = S:SECURE C:SECURE-OK
 * use-connection      = *channel
 * close-connection    = C:CLOSE S:CLOSE-OK
 *                     / S:CLOSE C:CLOSE-OK
 */
object Connection extends AMQClass {
  val id = 10
  val name = "connection"

  @throws(classOf[IOException])
  def readFrom(in: DataInputStream): Method = {
    val rdr = new ArgumentsReader(new ValueReader(in))
    in.readShort() match {
      case 10  => Start(rdr.readOctet(), rdr.readOctet(), rdr.readTable(), rdr.readLongstr(), rdr.readLongstr())
      case 11  => StartOk(rdr.readTable(), rdr.readShortstr(), rdr.readLongstr(), rdr.readShortstr())
      case 20  => Secure(rdr.readLongstr())
      case 21  => SecureOk(rdr.readLongstr())
      case 30  => Tune(rdr.readShort(), rdr.readLong(), rdr.readShort())
      case 31  => TuneOk(rdr.readShort(), rdr.readLong(), rdr.readShort())
      case 40  => Open(rdr.readShortstr(), rdr.readShortstr(), rdr.readBit())
      case 41  => OpenOk(rdr.readShortstr())
      case 50  => Close(rdr.readShort(), rdr.readShortstr(), rdr.readShort(), rdr.readShort())
      case 51  => CloseOk
      case 60  => Blocked(rdr.readShortstr())
      case 61  => Unblocked
      case mId => throw new UnknownClassOrMethodId(id, mId)
    }
  }

  final case class Start(versionMajor: Int, versionMinor: Int, serverProperties: Map[String, Any], mechanisms: LongString, locales: LongString) extends Method(10, "start") {
    assert(locales != null, nonNull("locales"))
    assert(mechanisms != null, nonNull("mechanisms"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeOctet(this.versionMajor)
      writer.writeOctet(this.versionMinor)
      writer.writeTable(this.serverProperties)
      writer.writeLongstr(this.mechanisms)
      writer.writeLongstr(this.locales)
    }
  }

  final case class StartOk(clientProperties: Map[String, Any], mechanism: String, response: LongString, locale: String) extends Method(11, "start-ok") {
    assert(locale != null, nonNull("locale"))
    assert(mechanism != null, nonNull("mechanism"))
    assert(response != null, nonNull("response"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeTable(this.clientProperties)
      writer.writeShortstr(this.mechanism)
      writer.writeLongstr(this.response)
      writer.writeShortstr(this.locale)
    }
  }

  final case class Secure(challenge: LongString) extends Method(20, "secure") {
    assert(challenge != null, nonNull("challenge"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeLongstr(this.challenge)
    }
  }

  final case class SecureOk(response: LongString) extends Method(21, "secure-ok") {
    assert(response != null, nonNull("response"))

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeLongstr(this.response)
    }
  }

  final case class Tune(channelMax: Int, frameMax: Int, heartbeat: Int) extends Method(30, "tune") {

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShort(this.channelMax)
      writer.writeLong(this.frameMax)
      writer.writeShort(this.heartbeat)
    }
  }

  final case class TuneOk(channelMax: Int, frameMax: Int, heartbeat: Int) extends Method(31, "tune-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShort(this.channelMax)
      writer.writeLong(this.frameMax)
      writer.writeShort(this.heartbeat)
    }
  }

  final case class Open(virtualHost: String, capabilities: String, insist: Boolean) extends Method(40, "open") {
    assert(virtualHost != null, nonNull("virtualHost"))
    assert(capabilities != null, nonNull("capabilities"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShortstr(this.virtualHost)
      writer.writeShortstr(this.capabilities)
      writer.writeBit(this.insist)
    }
  }

  final case class OpenOk(knownHosts: String) extends Method(41, "open-ok") {
    assert(knownHosts != null, nonNull("knownHosts"))

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShortstr(this.knownHosts)
    }
  }

  final case class Close(replyCode: Int, replyText: String, causeClassId: Int, causeMethodId: Int) extends Method(50, "close") {
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

  case object CloseOk extends Method(51, "close-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
    }
  }

  final case class Blocked(reason: String) extends Method(60, "blocked") {
    assert(reason != null, nonNull("reason"))

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShortstr(this.reason)
    }
  }

  case object Unblocked extends Method(61, "unblocked") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
    }
  }
}