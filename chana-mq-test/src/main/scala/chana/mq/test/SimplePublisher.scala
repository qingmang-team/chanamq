package chana.mq.test

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.ConnectionFactory

object SimplePublisher {
  val exchange = "test_exchange"
  val queueName = "test_queue"
  val routingKey = "quote"

  def main(args: Array[String]) {
    println("start ...")
    //Thread.sleep(5000)

    val factory = new ConnectionFactory()
    //factory.setUsername(userName)
    //factory.setPassword(password)
    //factory.setVirtualHost(virtualHost)
    //factory.setHost(hostName)
    //factory.setPort(5673)
    val conn = factory.newConnection()
    val channel = conn.createChannel()

    val res = channel.exchangeDeclare(exchange, "direct", true)
    println("res: " + res)

    val args = new java.util.HashMap[String, AnyRef]()
    args.put("x-message-ttl", 60000.asInstanceOf[AnyRef])

    val queue = channel.queueDeclare(queueName, true, false, false, args).getQueue()
    println(s"declare queue: $queue")

    val props1 = new BasicProperties.Builder().deliveryMode(2).build()
    val props2 = new BasicProperties.Builder().deliveryMode(2).expiration("100000").build()

    channel.queueBind(queue, exchange, routingKey)

    channel.basicPublish(exchange, routingKey, props1, "Hello, world0".getBytes)
    println("published")
    //Thread.sleep(3000)
    channel.basicPublish(exchange, routingKey, props2, "Hello, world1".getBytes)
    println("published")
    //Thread.sleep(3000)
    channel.basicPublish(exchange, routingKey, null, "Hello, world2".getBytes)
    println("published")
    //Thread.sleep(3000)
    channel.basicPublish(exchange, routingKey, null, "Hello, world3".getBytes)
    println("published")
    //Thread.sleep(3000)
    channel.basicPublish(exchange, routingKey, null, "Hello, world4".getBytes)
    println("published")

    Thread.sleep(3000)
    println("closing ...")
    channel.close()
    conn.close()
  }
}