package chana.mq.amqp.model

import com.typesafe.config.ConfigFactory
import java.util.concurrent.ConcurrentHashMap

object VirtualHost {
  val SEP = ConfigFactory.load().getString("chana.mq.amqp.vhost-seperator")

  private val virtualHosts = new ConcurrentHashMap[String, VirtualHost]()
  def getVirtualHost(id: String) = Option(virtualHosts.get(id))
  def createVirtualHost(id: String) = {
    val vh = VirtualHost(id, true)
    virtualHosts.putIfAbsent(id, vh) // TODO
    vh
  }

  def isDefault(id: String) = id.isEmpty
}
final case class VirtualHost(val id: String, val isActive: Boolean) {
  def authoriseCreateConnection(conn: AMQConnection): Boolean = true // TODO
}