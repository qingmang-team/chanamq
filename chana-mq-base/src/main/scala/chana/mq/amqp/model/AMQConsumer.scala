package chana.mq.amqp.model

import scala.collection.mutable

object AMQConsumer {
  private val EmptyMsgSet = mutable.Set[Long]()

  def globalId(tag: String, connectionId: Int, channelId: Int) = s"$connectionId-$channelId-$tag"
}
final case class AMQConsumer(channel: AMQChannel, tag: String, queue: String, autoAck: Boolean) {
  override def equals(x: Any) = x match {
    case c: AMQConsumer => c.tag == this.tag
    case _              => false
  }

  override def hashCode = tag.hashCode

  val unackedMessages = if (autoAck) AMQConsumer.EmptyMsgSet else new mutable.HashSet[Long]()
  var nUnacks = 0
}