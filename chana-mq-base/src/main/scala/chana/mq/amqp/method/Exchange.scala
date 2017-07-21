package chana.mq.amqp.method

import chana.mq.amqp.model.ValueReader
import java.io.DataInputStream
import java.io.IOException

/*-
 * The exchange class lets an application manage exchanges on the server. This class lets the application
 * script its own wiring (rather than relying on some configuration interface). Note: Most applications do not
 * need this level of sophistication, and legacy middleware is unlikely to be able to support this semantic.
 * The exchange life-cycle is:
 * 1. The client asks the server to make sure the exchange exists (Declare). The client can refine this into,
 * "create the exchange if it does not exist", or "warn me but do not create it, if it does not exist".
 * 2. The client publishes messages to the exchange.
 * 3. The client may choose to delete the exchange (Delete).
 * 
 * exchange            = C:DECLARE  S:DECLARE-OK
 *                     / C:DELETE   S:DELETE-OK
 *                     / C:BIND     S:BIND-OK
 *                     / C:UNBIND   S:UNBIND-OK
 *
 */
object Exchange extends AMQClass {
  val id = 40
  val name = "exchange"

  @throws(classOf[IOException])
  def readFrom(in: DataInputStream): Method = {
    val rdr = new ArgumentsReader(new ValueReader(in))
    in.readShort() match {
      case 10  => Declare(rdr.readShort(), rdr.readShortstr(), rdr.readShortstr(), rdr.readBit(), rdr.readBit(), rdr.readBit(), rdr.readBit(), rdr.readBit(), rdr.readTable())
      case 11  => DeclareOk
      case 20  => Delete(rdr.readShort(), rdr.readShortstr(), rdr.readBit(), rdr.readBit())
      case 21  => DeleteOk
      case 30  => Bind(rdr.readShort(), rdr.readShortstr(), rdr.readShortstr(), rdr.readShortstr(), rdr.readBit(), rdr.readTable())
      case 31  => BindOk
      case 40  => Unbind(rdr.readShort(), rdr.readShortstr(), rdr.readShortstr(), rdr.readShortstr(), rdr.readBit(), rdr.readTable())
      case 51  => UnbindOk
      case mId => throw new UnknownClassOrMethodId(id, mId)
    }
  }

  final case class Declare(reserved_1: Int, exchange: String, tpe: String, passive: Boolean, durable: Boolean, autoDelete: Boolean, internal: Boolean, nowait: Boolean, arguments: Map[String, Any]) extends Method(10, "declare") {
    assert(exchange != null, nonNull("exchange"))
    assert(tpe != null, nonNull("tpe"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShort(this.reserved_1)
      writer.writeShortstr(this.exchange)
      writer.writeShortstr(this.tpe)
      writer.writeBit(this.passive)
      writer.writeBit(this.durable)
      writer.writeBit(this.autoDelete)
      writer.writeBit(this.internal)
      writer.writeBit(this.nowait)
      writer.writeTable(this.arguments)
    }
  }

  case object DeclareOk extends Method(11, "declare-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
    }
  }

  final case class Delete(reserved_1: Int, exchange: String, ifUnused: Boolean, nowait: Boolean) extends Method(20, "delete") {
    assert(exchange != null, nonNull("exchange"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShort(this.reserved_1)
      writer.writeShortstr(this.exchange)
      writer.writeBit(this.ifUnused)
      writer.writeBit(this.nowait)
    }
  }

  case object DeleteOk extends Method(21, "delete-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
    }
  }

  final case class Bind(reserved_1: Int, destination: String, source: String, routingKey: String, nowait: Boolean, arguments: Map[String, Any]) extends Method(30, "bind") {
    assert(destination != null, nonNull("destination"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShort(this.reserved_1)
      writer.writeShortstr(this.destination)
      writer.writeShortstr(this.source)
      writer.writeShortstr(this.routingKey)
      writer.writeBit(this.nowait)
      writer.writeTable(this.arguments)
    }
  }

  case object BindOk extends Method(31, "bind-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
    }
  }

  final case class Unbind(reserved_1: Int, destination: String, source: String, routingKey: String, nowait: Boolean, arguments: Map[String, Any]) extends Method(40, "unbind") {
    assert(destination != null, nonNull("destination"))
    assert(routingKey != null, nonNull("routingKey"))
    assert(source != null, nonNull("source"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShort(this.reserved_1)
      writer.writeShortstr(this.destination)
      writer.writeShortstr(this.source)
      writer.writeShortstr(this.routingKey)
      writer.writeBit(this.nowait)
      writer.writeTable(this.arguments)
    }
  }

  case object UnbindOk extends Method(51, "unbind-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
    }
  }
}
