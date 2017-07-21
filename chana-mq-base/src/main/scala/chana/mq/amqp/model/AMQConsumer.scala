package chana.mq.amqp.model

import scala.collection.mutable

object AMQConsumer {
  private val EmptyMsgSet = mutable.Set[Long]()
}
final case class AMQConsumer(channel: AMQChannel, consumerTag: String, queue: String, autoAck: Boolean) {
  override def equals(x: Any) = x match {
    case c: AMQConsumer => c.consumerTag == this.consumerTag
    case _              => false
  }

  override def hashCode = consumerTag.hashCode

  val unackedMessages = if (autoAck) AMQConsumer.EmptyMsgSet else new mutable.HashSet[Long]()
  var nUnacks = 0
}