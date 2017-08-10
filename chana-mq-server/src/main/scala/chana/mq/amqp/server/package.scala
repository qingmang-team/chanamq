package chana.mq.amqp

import chana.mq.amqp.model.VirtualHost
import java.util.concurrent.ThreadLocalRandom

package object server {

  def nextRandomInt(max: Int) = ThreadLocalRandom.current.nextInt(max + 1)
  def nextRandomInt(min: Int, max: Int) = min + ThreadLocalRandom.current.nextInt(max - min + 1)

  trait Command extends Serializable {
    def id: String
  }
  trait VHostCommand extends Serializable {
    def vhost: String
    def id: String
    def entityId = if (vhost.isEmpty) {
      id
    } else {
      s"${vhost}${VirtualHost.sep}${id}"
    }
  }

  private[amqp] case object ActiveCheckTickKey
  private[amqp] case object ActiveCheckTick
  private[amqp] case object Loaded
  private[amqp] case object Unlock
}
