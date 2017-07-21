package chana.mq.amqp.model

import java.util.UUID

final case class VirtualHost(id: UUID, isActive: Boolean) {
  def authoriseCreateConnection(conn: AMQConnection): Boolean = true
}