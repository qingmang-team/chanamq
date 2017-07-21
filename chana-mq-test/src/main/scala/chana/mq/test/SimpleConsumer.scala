package chana.mq.test

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import java.io.IOException

object SimpleConsumer {
  def main(args: Array[String]) {
    println("start ...")
    //Thread.sleep(5000)

    val factory = new ConnectionFactory()
    //factory.setUsername(userName)
    //factory.setPassword(password)
    //factory.setVirtualHost(virtualHost)
    //factory.setHost(hostName)
    //factory.setPort(portNumber)
    val conn = factory.newConnection()

    val channel = conn.createChannel()

    val exchange = SimplePublisher.exchange
    val queue = SimplePublisher.queueName
    val routingKey = SimplePublisher.routingKey

    // --- consumer
    println("going to consume...")

    //var getResp: GetResponse = null
    //getResp = channel.basicGet(queue, true)
    //if (getResp != null) println(s"Got ${new String(getResp.getBody)} $getResp")

    //    Thread.sleep(3000)
    //    getResp = channel.basicGet(queue, true)
    //    if (getResp != null) println(s"Got ${new String(getResp.getBody)} $getResp")
    //
    //    Thread.sleep(3000)
    //    getResp = channel.basicGet(queue, false)
    //    if (getResp != null) println(s"Got ${new String(getResp.getBody)} $getResp")

    val consumer = new DefaultConsumer(channel) {
      @throws(classOf[IOException])
      override def handleDelivery(
        consumerTag: String,
        envelope:    Envelope,
        properties:  AMQP.BasicProperties,
        body:        Array[Byte]
      ) {
        val routingKey = envelope.getRoutingKey
        val contentType = properties.getContentType
        val deliveryTag = envelope.getDeliveryTag
        println(s"Got message: ${new String(body)}")
        // (process the message components here ...)
        //getChannel.basicAck(deliveryTag, false)
      }
    }

    channel.basicConsume(queue, true, "myConsumerTag", consumer)

    Thread.sleep(20000)
    println("closing ...")
    channel.close()
    conn.close()
  }
}