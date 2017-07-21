package chana.mq.amqp.method

import chana.mq.amqp.model.ValueReader
import java.io.DataInputStream
import java.io.IOException

object Access extends AMQClass {
  val id = 30
  val name = "access"

  @throws(classOf[IOException])
  def readFrom(in: DataInputStream): Method = {
    val rdr = new ArgumentsReader(new ValueReader(in))
    in.readShort() match {
      case 10  => Request(rdr.readShortstr(), rdr.readBit(), rdr.readBit(), rdr.readBit(), rdr.readBit(), rdr.readBit())
      case 11  => RequestOk(rdr.readShort())
      case mId => throw new UnknownClassOrMethodId(id, mId)
    }
  }

  final case class Request(realm: String, exclusive: Boolean, passive: Boolean, active: Boolean, write: Boolean, read: Boolean) extends Method(10, "request") {
    assert(realm != null, nonNull("realm"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShortstr(this.realm)
      writer.writeBit(this.exclusive)
      writer.writeBit(this.passive)
      writer.writeBit(this.active)
      writer.writeBit(this.write)
      writer.writeBit(this.read)
    }
  }

  final case class RequestOk(reserved_1: Int) extends Method(11, "request-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShort(this.reserved_1)
    }
  }
}