package chana.mq.amqp.model

import chana.mq.amqp.method.AMQClass
import scala.collection.mutable

object AMQChannel {
  private type AMQMethod = AMQClass#Method

  sealed trait Mode
  case object Normal extends Mode
  case object Transaction extends Mode
  case object Confirm extends Mode

  final case class DeliveryMsg(deliveryTag: Long, msgId: Long, consumer: AMQConsumer, nDelivery: Int)
}
class AMQChannel(val connection: AMQConnection, val id: Int) {
  import AMQChannel._

  var mode: Mode = Normal

  var isDoingPublishing = false

  private var confirmNumber = 1L
  def nextConfirmNumber() = {
    val n = confirmNumber
    confirmNumber += 1
    n
  }

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
  val unackedDeliveryTagToMsg = new mutable.LinkedHashMap[Long, DeliveryMsg]()
  /**
   * TODO
   * If nDelivery is more than for example 5 times, we should consider that this message
   * will never to be delivered and drop it
   */
  val unackedMsgIdToDeliveryMsg = new mutable.HashMap[Long, DeliveryMsg]()

  def nUnacks = unackedDeliveryTagToMsg.size

  def removeUnReferredMsgId() {
    val msgIdsToRemove = unackedMsgIdToDeliveryMsg.collect {
      case (msgId, msg) if !unackedDeliveryTagToMsg.contains(msg.deliveryTag) => msgId
    }

    unackedMsgIdToDeliveryMsg --= msgIdsToRemove
  }

  /**
   * @return generated channel specific delivery tags
   */
  def goingToDeliveryMsgs(msgIds: Vector[Long], consumer: AMQConsumer, autoAck: Boolean): Vector[DeliveryMsg] = {
    msgIds.map { msgId =>
      val tag = deliveryTag
      deliveryTag += 1
      val msg = unackedMsgIdToDeliveryMsg.get(msgId) match {
        case Some(msg) => DeliveryMsg(tag, msgId, consumer, msg.nDelivery + 1)
        case None      => DeliveryMsg(tag, msgId, consumer, 1)
      }

      if (!autoAck) {
        consumer.nUnacks += 1
        unackedDeliveryTagToMsg += (tag -> msg)
        unackedMsgIdToDeliveryMsg += (msgId -> msg)
      }

      msg
    }
  }

  def ackDeliveryTag(deliveryTag: Long): Option[(String, Long)] = {
    val queueAndMsgId = msgOfDeliveryTag(deliveryTag) map {
      case DeliveryMsg(_, msgId, consumer, _) =>
        consumer.nUnacks -= 1
        unackedMsgIdToDeliveryMsg -= msgId

        (consumer.queue, msgId)
    }

    unackedDeliveryTagToMsg -= deliveryTag

    queueAndMsgId
  }

  def ackDeliveryTags(tags: collection.Set[Long]): collection.Set[(String, Long)] = {
    val queueAndMsgIds = tags map { tag =>
      msgOfDeliveryTag(tag) map {
        case DeliveryMsg(_, msgId, consumer, _) =>
          consumer.nUnacks -= 1
          unackedMsgIdToDeliveryMsg -= msgId

          (consumer.queue, msgId)
      }
    }

    unackedDeliveryTagToMsg --= tags

    queueAndMsgIds.flatten
  }

  def msgOfDeliveryTag(tag: Long): Option[DeliveryMsg] =
    unackedDeliveryTagToMsg.get(tag)

  def getMultipleTagsTill(diliveryTag: Long): collection.Set[Long] = {
    var tags = Set[Long]()
    var break = false
    val unackedTags = unackedDeliveryTagToMsg.keysIterator
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
}

class AlreadyClosedException(reason: String) extends Exception(reason)