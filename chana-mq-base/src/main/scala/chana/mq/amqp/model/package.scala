package chana.mq.amqp

import chana.mq.amqp.model.BasicProperties

package object model {

  private[amqp] final case class Exchange(tpe: String, isDurable: Boolean, isAutoDelete: Boolean, isInternal: Boolean, args: Map[String, String], binds: List[Bind])
  private[amqp] final case class Bind(vhost: String, exchange: String, queue: String, routingKey: String, arguments: Map[String, Any])
  private[amqp] final case class Message(id: Long, header: Option[BasicProperties], body: Option[Array[Byte]], exchange: String, routingKey: String, ttl: Option[Long])
  private[amqp] final case class Queue(lastConsumed: Long, consumers: Set[String], isDurable: Boolean, queueMsgTtl: Option[Long], mags: Vector[Msg], unacks: Vector[Msg])
  /** message representation in queue */
  private[amqp] final case class Msg(id: Long, offset: Long, bodySize: Int, expireTime: Option[Long]) {
    def expired(now: Long): Boolean = expireTime.map(_ < now).getOrElse(false)
  }
}
