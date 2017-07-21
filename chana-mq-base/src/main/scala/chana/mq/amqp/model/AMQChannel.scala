package chana.mq.amqp.model

import akka.util.ByteString
import chana.mq.amqp.method.AMQClass
import java.io.DataOutputStream
import java.io.IOException
import scala.collection.mutable

class AMQChannel(val connection: AMQConnection, val channelNumber: Int) {
  private type AMQMethod = AMQClass#Method

  val consumers = new mutable.Queue[AMQConsumer]()

  /**
   * The consumer which is used for Basic.Get method
   */
  val basicGetConsumer = AMQConsumer(this, "", "", autoAck = false)

  /**
   * prefetchSize - maximum amount of content (measured in octets) that the server will deliver
   * prefetchCount - maximum number of messages that the server will deliver
   *
   * +--------+-----------------------------------------------+--------------------------------------------------------+
   * | global	| Meaning of prefetch_count in AMQP 0-9-1	      | Meaning of prefetch_count in RabbitMQ                  |
   * +========+===============================================+========================================================+
   * | false	| shared across all consumers on the channel	  | applied separately to each new consumer on the channel |
   * +--------+-----------------------------------------------+--------------------------------------------------------+
   * | true	  | shared across all consumers on the connection |	shared across all consumers on the channel             |
   * +--------+-----------------------------------------------+--------------------------------------------------------+
   */
  var prefetchCount = Int.MaxValue
  var prefetchSize = Long.MaxValue
  var prefetchGlobal: Boolean = _

  var isOpen: Boolean = true

  @throws(classOf[AlreadyClosedException])
  def ensureIsOpen() {
    if (!isOpen) {
      throw new AlreadyClosedException("Channel or Connection already closed")
    }
  }

  @throws(classOf[IOException])
  def render(c: AMQCommand[_ <: AMQMethod]): ByteString = {
    ensureIsOpen()
    // TODO write to outlet of FrameHandler directly
    val buf = ByteString.newBuilder
    val os = new DataOutputStream(buf.asOutputStream)
    render(c, os)
    os.flush()
    buf.result
  }

  @throws(classOf[IOException])
  private def render(command: AMQCommand[_ <: AMQMethod], os: DataOutputStream) {
    val methodFrame = command.method.toFrame(channelNumber)
    methodFrame.writeTo(os)
    if (command.method.hasContent) {
      command match {
        case AMQCommand(method, Some(contentHeader), Some(contentBody), _) =>
          val headerFrame = contentHeader.toFrame(channelNumber, contentBody.length)
          headerFrame.writeTo(os)

          val frameMax = connection.frameMax
          val bodyPayloadMax = if (frameMax == 0) contentBody.length else frameMax - Frame.NON_BODY_SIZE

          var offset = 0
          while (offset < contentBody.length) {
            val remaining = contentBody.length - offset

            val fragmentLength = if (remaining < bodyPayloadMax) remaining else bodyPayloadMax
            val bodyFrame = Frame.fromBodyFragment(channelNumber, contentBody, offset, fragmentLength)
            bodyFrame.writeTo(os)
            offset += bodyPayloadMax
          }

        case _ =>
          throw new RuntimeException(s"$command method has content, but with None header or None body")
      }
    }
  }
}

class AlreadyClosedException(reason: String) extends Exception(reason)