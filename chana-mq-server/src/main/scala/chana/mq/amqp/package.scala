package chana.mq

import chana.mq.amqp.entity.ExchangeEntity
import chana.mq.amqp.model.BasicProperties
import java.time.ZoneId
import java.util.concurrent.ThreadLocalRandom

/**
 * @author Caoyuan Deng
 */
package object amqp {
  val zoneId = ZoneId.of("Asia/Shanghai")

  def nextRandomInt(max: Int) = ThreadLocalRandom.current.nextInt(max + 1)
  def nextRandomInt(min: Int, max: Int) = min + ThreadLocalRandom.current.nextInt(max - min + 1)

  trait Command extends Serializable { def id: String }

  private[amqp] case object ActiveCheckTick
  private[amqp] case object Loaded
  private[amqp] case object Unlock

  private[amqp] final case class Exchange(tpe: String, isDurable: Boolean, isAutoDelete: Boolean, isInternal: Boolean, args: Map[String, String], binds: List[ExchangeEntity.QueueBind])
  private[amqp] final case class Msg(id: Long, offset: Long, bodySize: Int, expireTime: Option[Long]) {
    def expired(now: Long): Boolean = expireTime.map(_ < now).getOrElse(false)
  }
  private[amqp] final case class Message(id: Long, header: Option[BasicProperties], body: Option[Array[Byte]], exchange: String, routingKey: String, ttl: Option[Long])
  private[amqp] final case class Queue(lastConsumed: Long, consumerCount: Int, isDurable: Boolean, queueMsgTtl: Option[Long], mags: Vector[Msg], unacks: Vector[Long])
}
