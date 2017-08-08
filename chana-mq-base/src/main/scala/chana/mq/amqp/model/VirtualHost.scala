package chana.mq.amqp.model

import com.typesafe.config.ConfigFactory

object VirtualHost {
  val (defaultId, sep) = {
    val config = ConfigFactory.load().getConfig("chana.mq.amqp.vhost")
    (config.getString("default-id"), config.getString("seperator"))
  }
}
final case class VirtualHost(id: String, isActive: Boolean) {
  def authoriseOpenConnection(connection: AMQConnection) = true // TODO
}