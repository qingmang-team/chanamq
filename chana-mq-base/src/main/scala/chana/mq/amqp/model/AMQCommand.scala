package chana.mq.amqp.model

import chana.mq.amqp.method.AMQClass

final case class AMQCommand[T <: AMQClass#Method](method: T, contentHeader: Option[BasicProperties] = None, contentBody: Option[Array[Byte]] = None, channelId: Int = -1) {
  assert(method.hasContent && contentHeader.isDefined && contentBody.isDefined || !method.hasContent, s"$this should be: method.hasContent && contentHeader.isDefined && contentBody.isDefined || !method.hasContent ")

  def toStringWithBody: String = {
    val sb = new StringBuilder()
    sb.append("AMQCommand(")
      .append(method).append(", ")
      .append(contentHeader).append(", ")

    val body = contentBody map { x =>
      try {
        new String(x, "UTF-8")
      } catch {
        case e: Exception => "|" + x.length + "|"
      }
    }

    sb.append(body).append(')')
    sb.toString
  }
}
