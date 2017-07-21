package chana.mq.amqp.method

import chana.mq.amqp.model.ValueReader
import java.io.DataInputStream
import java.io.IOException

/*-
 * The Basic class implements the messaging capabilities described in this specification. It supports these
 * main semantics:
 * # Sending messages from client to server, which happens asynchronously (Publish)
 * # Starting and stopping consumers (Consume, Cancel)
 * # Sending messages from server to client, which happens asynchronously (Deliver, Return)
 * # Acknowledging messages (Ack, Reject)
 * # Taking messages off the message queue synchronously (Get).
 *
 * basic               = C:QOS S:QOS-OK
 *                     / C:CONSUME S:CONSUME-OK
 *                     / C:CANCEL S:CANCEL-OK
 *                     / C:PUBLISH content
 *                     / S:RETURN content
 *                     / S:DELIVER content
 *                     / C:GET ( S:GET-OK content / S:GET-EMPTY )
 *                     / C:ACK
 *                     / S:ACK
 *                     / C:REJECT
 *                     / C:NACK
 *                     / S:NACK
 *                     / C:RECOVER-ASYNC
 *                     / C:RECOVER S:RECOVER-OK
 */
object Basic extends AMQClass {
  val id = 60
  val name = "basic"

  @throws(classOf[IOException])
  def readFrom(in: DataInputStream): Method = {
    val rdr = new ArgumentsReader(new ValueReader(in))
    in.readShort() match {
      case 10  => Qos(rdr.readLong(), rdr.readShort(), rdr.readBit())
      case 11  => QosOk
      case 20  => Consume(rdr.readShort(), rdr.readShortstr(), rdr.readShortstr(), rdr.readBit(), rdr.readBit(), rdr.readBit(), rdr.readBit(), rdr.readTable())
      case 21  => ConsumeOk(rdr.readShortstr())
      case 30  => Cancel(rdr.readShortstr(), rdr.readBit())
      case 31  => CancelOk(rdr.readShortstr())
      case 40  => Publish(rdr.readShort(), rdr.readShortstr(), rdr.readShortstr(), rdr.readBit(), rdr.readBit())
      case 50  => Return(rdr.readShort(), rdr.readShortstr(), rdr.readShortstr(), rdr.readShortstr())
      case 60  => Deliver(rdr.readShortstr(), rdr.readLonglong(), rdr.readBit(), rdr.readShortstr(), rdr.readShortstr())
      case 70  => Get(rdr.readShort(), rdr.readShortstr(), rdr.readBit())
      case 71  => GetOk(rdr.readLonglong(), rdr.readBit(), rdr.readShortstr(), rdr.readShortstr(), rdr.readLong())
      case 72  => GetEmpty(rdr.readShortstr())
      case 80  => Ack(rdr.readLonglong(), rdr.readBit())
      case 90  => Reject(rdr.readLonglong(), rdr.readBit())
      case 100 => RecoverAsync(rdr.readBit())
      case 110 => Recover(rdr.readBit())
      case 111 => RecoverOk
      case 120 => Nack(rdr.readLonglong(), rdr.readBit(), rdr.readBit())
      case mId => throw new UnknownClassOrMethodId(id, mId)
    }
  }

  final case class Qos(prefetchSize: Int, prefetchCount: Int, global: Boolean) extends Method(10, "qos") {

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeLong(this.prefetchSize)
      writer.writeShort(this.prefetchCount)
      writer.writeBit(this.global)
    }
  }

  case object QosOk extends Method(11, "qos-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
    }
  }

  final case class Consume(reserved_1: Int, queue: String, consumerTag: String, noLocal: Boolean, noAck: Boolean, exclusive: Boolean, nowait: Boolean, arguments: Map[String, Any]) extends Method(20, "consume") {
    assert(consumerTag != null, nonNull("consumerTag"))
    assert(queue != null, nonNull("queue"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShort(this.reserved_1)
      writer.writeShortstr(this.queue)
      writer.writeShortstr(this.consumerTag)
      writer.writeBit(this.noLocal)
      writer.writeBit(this.noAck)
      writer.writeBit(this.exclusive)
      writer.writeBit(this.nowait)
      writer.writeTable(this.arguments)
    }
  }

  final case class ConsumeOk(consumerTag: String) extends Method(21, "consume-ok") {
    assert(consumerTag != null, nonNull("consumerTag"))

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShortstr(this.consumerTag)
    }
  }

  final case class Cancel(consumerTag: String, nowait: Boolean) extends Method(30, "cancel") {
    assert(consumerTag != null, nonNull("consumerTag"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShortstr(this.consumerTag)
      writer.writeBit(this.nowait)
    }
  }

  final case class CancelOk(consumerTag: String) extends Method(31, "cancel") {
    assert(consumerTag != null, nonNull("consumerTag"))

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShortstr(this.consumerTag)
    }
  }

  /**
   * bit mandatory
   * This flag tells the server how to react if the message cannot be routed to a queue.
   * If this flag is set, the server will return an unroutable message with a Return method.
   * If this flag is zero, the server silently drops the message.
   * The server SHOULD implement the mandatory flag.
   *
   * bit immediate
   * This flag tells the server how to react if the message cannot be routed to a queue
   * consumer immediately. If this flag is set, the server will return an undeliverable
   * message with a Return method. If this flag is zero, the server will queue the message,
   * but with no guarantee that it will ever be consumed.
   * The server SHOULD implement the immediate flag.
   */
  final case class Publish(reserved_1: Int, exchange: String, routingKey: String, mandatory: Boolean, immediate: Boolean) extends Method(40, "publish") {
    assert(exchange != null, nonNull("exchange"))
    assert(routingKey != null, nonNull("routingKey"))

    def hasContent = true
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShort(this.reserved_1)
      writer.writeShortstr(this.exchange)
      writer.writeShortstr(this.routingKey)
      writer.writeBit(this.mandatory)
      writer.writeBit(this.immediate)
    }
  }

  final case class Return(replyCode: Int, replyText: String, exchange: String, routingKey: String) extends Method(50, "return") {
    assert(exchange != null, nonNull("exchange"))
    assert(replyText != null, nonNull("replyText"))
    assert(routingKey != null, nonNull("routingKey"))

    def hasContent = true
    def expectResponse = false // TODO

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShort(this.replyCode)
      writer.writeShortstr(this.replyText)
      writer.writeShortstr(this.exchange)
      writer.writeShortstr(this.routingKey)
    }
  }

  final case class Deliver(consumerTag: String, deliveryTag: Long, redelivered: Boolean, exchange: String, routingKey: String) extends Method(60, "deliver") {
    assert(consumerTag != null, nonNull("consumerTag"))
    assert(exchange != null, nonNull("exchange"))
    assert(routingKey != null, nonNull("routingKey"))

    def hasContent = true
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShortstr(this.consumerTag)
      writer.writeLonglong(this.deliveryTag)
      writer.writeBit(this.redelivered)
      writer.writeShortstr(this.exchange)
      writer.writeShortstr(this.routingKey)
    }
  }

  final case class Get(reserved_1: Int, queue: String, noAck: Boolean) extends Method(70, "get") {
    assert(queue != null, nonNull("queue"))

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShort(this.reserved_1)
      writer.writeShortstr(this.queue)
      writer.writeBit(this.noAck)
    }
  }

  final case class GetOk(deliveryTag: Long, redelivered: Boolean, exchange: String, routingKey: String, messageCount: Int) extends Method(71, "get-ok") {
    assert(exchange != null, nonNull("exchange"))
    assert(routingKey != null, nonNull("routingKey"))

    def hasContent = true
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeLonglong(this.deliveryTag)
      writer.writeBit(this.redelivered)
      writer.writeShortstr(this.exchange)
      writer.writeShortstr(this.routingKey)
      writer.writeLong(this.messageCount)
    }
  }

  final case class GetEmpty(clusterId: String) extends Method(72, "get-empty") {
    assert(clusterId != null, nonNull("clusterId"))

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeShortstr(this.clusterId)
    }
  }

  final case class Ack(deliveryTag: Long, multiple: Boolean) extends Method(80, "ack") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeLonglong(this.deliveryTag)
      writer.writeBit(this.multiple)
    }
  }

  final case class Reject(deliveryTag: Long, requeue: Boolean) extends Method(90, "reject") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeLonglong(this.deliveryTag)
      writer.writeBit(this.requeue)
    }
  }

  final case class RecoverAsync(requeue: Boolean) extends Method(100, "recover.async") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeBit(this.requeue)
    }
  }

  final case class Recover(requeue: Boolean) extends Method(110, "recover") {

    def hasContent = false
    def expectResponse = true

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeBit(this.requeue)
    }
  }

  case object RecoverOk extends Method(111, "recover-ok") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
    }
  }

  final case class Nack(deliveryTag: Long, multiple: Boolean, requeue: Boolean) extends Method(120, "nack") {

    def hasContent = false
    def expectResponse = false

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter) {
      writer.writeLonglong(this.deliveryTag)
      writer.writeBit(this.multiple)
      writer.writeBit(this.requeue)
    }
  }
}
