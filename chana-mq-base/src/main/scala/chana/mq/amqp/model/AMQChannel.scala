package chana.mq.amqp.model

import akka.util.ByteString
import chana.mq.amqp.method.AMQClass
import java.io.DataOutputStream
import java.io.IOException
import scala.collection.mutable

class AMQChannel(val connection: AMQConnection, val channelNumber: Int) {
  private type AMQMethod = AMQClass#Method

  val consumers = new mutable.Queue[AMQConsumer]()

  /**
   * The consumer which is used for Basic.Get method
   */
  val basicGetConsumer = AMQConsumer(this, "", "", autoAck = false)

  /**
   * prefetchSize - maximum amount of content (measured in octets) that the server will deliver
   * prefetchCount - maximum number of messages that the server will deliver
   *
   * +--------+-----------------------------------------------+--------------------------------------------------------+
   * | global	| Meaning of prefetch_count in AMQP 0-9-1	      | Meaning of prefetch_count in RabbitMQ                  |
   * +========+===============================================+========================================================+
   * | false	| shared across all consumers on the channel	  | applied separately to each new consumer on the channel |
   * +--------+-----------------------------------------------+--------------------------------------------------------+
   * | true	  | shared across all consumers on the connection |	shared across all consumers on the channel             |
   * +--------+-----------------------------------------------+--------------------------------------------------------+
   */
  var prefetchCount = Int.MaxValue
  var prefetchSize = Long.MaxValue
  var prefetchGlobal: Boolean = _

  var isOpen: Boolean = true

  /**
   * The server-assigned and channel-specific delivery tag
   *
   * The delivery tag is valid only within the channel from which the message was
   * received. I.e. a client MUST NOT receive a message on one channel and then
   * acknowledge it on another.
   *
   * The server MUST NOT use a zero value for delivery tags. Zero is reserved
   * for client use, meaning "all messages so far received".
   */
  private var deliveryTag = 1L

  /**
   * Use LinkedHashMap to keep the original inserting order
   */
  private val unackedDeliveryTagToMsgId = mutable.LinkedHashMap[Long, (Long, AMQConsumer)]()

  def nUnacks = unackedDeliveryTagToMsgId.size

  /**
   * @return generated channel specific delivery tags
   */
  def goingToDeliveryMsgs(msgIds: Vector[Long], consumer: AMQConsumer, autoAck: Boolean): Vector[Long] = {
    msgIds.map { msgId =>
      val tag = deliveryTag
      if (!autoAck) {
        consumer.nUnacks += 1
        unackedDeliveryTagToMsgId += (tag -> (msgId, consumer))
      }
      deliveryTag += 1
      tag
    }
  }

  def ackDeliveryTag(deliveryTag: Long): Option[(String, Long)] = {
    val queueAndMsgId = msgIdOfDeliveryTag(deliveryTag) map {
      case (msgId, consumer) =>
        consumer.nUnacks -= 1
        (consumer.queue, msgId)
    }

    unackedDeliveryTagToMsgId -= deliveryTag

    queueAndMsgId
  }

  def ackDeliveryTags(tags: collection.Set[Long]): collection.Set[(String, Long)] = {
    val queueAndMsgIds = tags map { tag =>
      msgIdOfDeliveryTag(tag) map {
        case (msgId, consumer) =>
          consumer.nUnacks -= 1
          (consumer.queue, msgId)
      }
    }

    unackedDeliveryTagToMsgId --= tags

    queueAndMsgIds.flatten
  }

  def msgIdOfDeliveryTag(tag: Long): Option[(Long, AMQConsumer)] =
    unackedDeliveryTagToMsgId.get(tag)

  def getMultipleTagsTill(diliveryTag: Long): collection.Set[Long] = {
    var tags = Set[Long]()
    var break = false
    val unackedTags = unackedDeliveryTagToMsgId.keysIterator
    while (unackedTags.hasNext && !break) {
      val tag = unackedTags.next()
      if (tag <= diliveryTag) {
        tags += tag
      } else {
        break = true
      }
    }
    tags
  }

  @throws(classOf[AlreadyClosedException])
  def ensureIsOpen() {
    if (!isOpen) {
      throw new AlreadyClosedException("Channel or Connection already closed")
    }
  }

  @throws(classOf[IOException])
  def render(c: AMQCommand[_ <: AMQMethod]): ByteString = {
    ensureIsOpen()
    // TODO write to outlet of FrameHandler directly
    val buf = ByteString.newBuilder
    val os = new DataOutputStream(buf.asOutputStream)
    render(c, os)
    os.flush()
    buf.result
  }

  @throws(classOf[IOException])
  private def render(command: AMQCommand[_ <: AMQMethod], os: DataOutputStream) {
    val methodFrame = command.method.toFrame(channelNumber)
    methodFrame.writeTo(os)
    if (command.method.hasContent) {
      command match {
        case AMQCommand(method, Some(contentHeader), Some(contentBody), _) =>
          val headerFrame = contentHeader.toFrame(channelNumber, contentBody.length)
          headerFrame.writeTo(os)

          val frameMax = connection.frameMax
          val bodyPayloadMax = if (frameMax == 0) contentBody.length else frameMax - Frame.NON_BODY_SIZE

          var offset = 0
          while (offset < contentBody.length) {
            val remaining = contentBody.length - offset

            val fragmentLength = if (remaining < bodyPayloadMax) remaining else bodyPayloadMax
            val bodyFrame = Frame.fromBodyFragment(channelNumber, contentBody, offset, fragmentLength)
            bodyFrame.writeTo(os)
            offset += bodyPayloadMax
          }

        case _ =>
          throw new RuntimeException(s"$command method has content, but with None header or None body")
      }
    }
  }
}

class AlreadyClosedException(reason: String) extends Exception(reason)