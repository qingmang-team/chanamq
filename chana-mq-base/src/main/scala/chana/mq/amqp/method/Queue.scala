package chana.mq.amqp.method

import chana.mq.amqp.model.ValueReader
import java.io.DataInputStream
import java.io.IOException

/*-
 * The queue class lets an application manage message queues on the server. This is a basic step in almost all
 * applications that consume messages, at least to verify that an expected message queue is actually present.
 * The life-cycle for a durable message queue is fairly simple:
 * 1. The client asserts that the message queue exists (Declare, with the "passive" argument).
 * 2. The server confirms that the message queue exists (Declare-Ok).
 * 3. The client reads messages off the message queue.
 *
 * The life-cycle for a temporary message queue is more interesting:
 * 1. The client creates the message queue (Declare, often with no message queue name so the server will
 * assign a name). The server confirms (Declare-Ok).
 * 2. The client starts a consumer on the message queue. The precise functionality of a consumer is defined
 * by the Basic class.
 * 3. The client cancels the consumer, either explicitly or by closing the channel and/or connection.
 * 4. When the last consumer disappears from the message queue, and after a polite time-out, the server
 * deletes the message queue.
 *
 * AMQP implements the delivery mechanism for topic subscriptions as message queues. This enables
 * interesting structures where a subscription can be load balanced among a pool of co-operating subscriber
 * applications.
 *
 * The life-cycle for a subscription involves an extra bind stage:
 * 1. The client creates the message queue (Declare), and the server confirms (Declare-Ok).
 * 2. The client binds the message queue to a topic exchange (Bind) and the server confirms (Bind-Ok).
 * 3. The client uses the message queue as in the previous examples.
 *
 * queue               = C:DECLARE  S:DECLARE-OK
 *                     / C:BIND     S:BIND-OK
 *                     / C:UNBIND   S:UNBIND-OK
 *                     / C:PURGE    S:PURGE-OK
 *                     / C:DELETE   S:DELETE-OK
 */
object Queue extends AMQClass {
  val id = 50
  val name = "queue"

  @throws(classOf[IOException])
  def readFrom(in: DataInputStream): Method = {
    val rdr = new ArgumentsReader(new ValueReader(in))
    in.readShort() match {
      case 10  => Declare(rdr.readShort(), rdr.readShortstr(), rdr.readBit(), rdr.readBit(), rdr.readBit(), rdr.readBit(), rdr.readBit(), rdr.readTable())
      case 11  => DeclareOk(rdr.readShortstr(), rdr.readLong(), rdr.readLong())
      case 20  => Bind(rdr.readShort(), rdr.readShortstr(), rdr.readShortstr(), rdr.readShortstr(), rdr.readBit(), rdr.readTable())
      case 21  => BindOk
      case 30  => Purge(rdr.readShort(), rdr.readShortstr(), rdr.readBit())
      case 31  => PurgeOk(rdr.readLong())
      case 40  => Delete(rdr.readShort(), rdr.readShortstr(), rdr.readBit(), rdr.readBit(), rdr.readBit())
      case 41  => DeleteOk(rdr.readLong())
      case 50  => Unbind(rdr.readShort(), rdr.readShortstr(), rdr.readShortstr(), rdr.readShortstr(), rdr.readTable())
      case 51  => UnbindOk
      case mId => throw new UnknownClassOrMethodId(id, mId)
    }
  }

  final case class Declare(reserved_1: Int, queue: String, passive: Boolean, durable: Boolean, exclusive: Boolean, autoDelete: Boolean, nowait: Boolean, arguments: Map[String, Any]) extends Method(10, "declare") {
    assert(queue != null, nonNull("queue"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShort(this.reserved_1)
      writer.writeShortstr(this.queue)
      writer.writeBit(this.passive)
      writer.writeBit(this.durable)
      writer.writeBit(this.exclusive)
      writer.writeBit(this.autoDelete)
      writer.writeBit(this.nowait)
      writer.writeTable(this.arguments)
    }
  }

  final case class DeclareOk(queue: String, messageCount: Int, consumerCount: Int) extends Method(11, "declare-ok") {
    assert(queue != null, nonNull("queue"))

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShortstr(this.queue)
      writer.writeLong(this.messageCount)
      writer.writeLong(this.consumerCount)
    }
  }

  final case class Bind(reserved_1: Int, queue: String, exchange: String, routingKey: String, nowait: Boolean, arguments: Map[String, Any]) extends Method(20, "bind") {
    assert(exchange != null, nonNull("exchange"))
    assert(queue != null, nonNull("queue"))
    assert(routingKey != null, nonNull("routingKey"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShort(this.reserved_1)
      writer.writeShortstr(this.queue)
      writer.writeShortstr(this.exchange)
      writer.writeShortstr(this.routingKey)
      writer.writeBit(this.nowait)
      writer.writeTable(this.arguments)
    }
  }

  case object BindOk extends Method(21, "bind-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
    }
  }

  final case class Unbind(reserved_1: Int, queue: String, exchange: String, routingKey: String, arguments: Map[String, Any]) extends Method(50, "unbind") {
    assert(exchange != null, nonNull("exchange"))
    assert(queue != null, nonNull("queue"))
    assert(routingKey != null, nonNull("routingKey"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShort(this.reserved_1)
      writer.writeShortstr(this.queue)
      writer.writeShortstr(this.exchange)
      writer.writeShortstr(this.routingKey)
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

  final case class Purge(reserved_1: Int, queue: String, nowait: Boolean) extends Method(30, "purge") {
    assert(queue != null, nonNull("queue"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShort(this.reserved_1)
      writer.writeShortstr(this.queue)
      writer.writeBit(this.nowait)
    }
  }

  final case class PurgeOk(messageCount: Int) extends Method(31, "purge-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeLong(this.messageCount)
    }
  }

  final case class Delete(reserved_1: Int, queue: String, ifUnused: Boolean, ifEmpty: Boolean, nowait: Boolean) extends Method(40, "delete") {
    assert(queue != null, nonNull("queue"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShort(this.reserved_1)
      writer.writeShortstr(this.queue)
      writer.writeBit(this.ifUnused)
      writer.writeBit(this.ifEmpty)
      writer.writeBit(this.nowait)
    }
  }

  final case class DeleteOk(messageCount: Int) extends Method(41, "delete-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeLong(this.messageCount)
    }
  }

}
