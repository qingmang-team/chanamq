package chana.mq.amqp.model

import akka.util.ByteString
import chana.mq.amqp.method.AMQClass
import java.io.DataOutputStream
import java.io.IOException

final case class AMQCommand[T <: AMQClass#Method](channel: AMQChannel, method: T, contentHeader: Option[BasicProperties] = None, contentBody: Option[Array[Byte]] = None) {
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

  @throws(classOf[IOException])
  def render: ByteString = {
    val buf = ByteString.newBuilder
    val os = new DataOutputStream(buf.asOutputStream)
    render(os)
    os.flush()
    buf.result
  }

  @throws(classOf[IOException])
  private def render(os: DataOutputStream) {
    val methodFrame = method.toFrame(channel.id)
    methodFrame.writeTo(os)
    if (method.hasContent) {
      (contentHeader, contentBody) match {
        case (Some(header), Some(body)) =>
          val headerFrame = header.toFrame(channel.id, body.length)
          headerFrame.writeTo(os)

          val frameMax = channel.connection.frameMax
          val bodyPayloadMax = if (frameMax == 0) body.length else frameMax - Frame.NON_BODY_SIZE

          var offset = 0
          while (offset < body.length) {
            val remaining = body.length - offset

            val fragmentLength = if (remaining < bodyPayloadMax) remaining else bodyPayloadMax
            val bodyFrame = Frame.fromBodyFragment(channel.id, body, offset, fragmentLength)
            bodyFrame.writeTo(os)
            offset += bodyPayloadMax
          }

        case _ =>
          throw new RuntimeException(s"$this method has content, but with None header or None body")
      }
    }
  }
}
