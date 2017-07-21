package chana.mq.amqp.server

import chana.mq.amqp.Exchange
import chana.mq.amqp.Message
import chana.mq.amqp.Queue
import chana.mq.amqp.model.BasicProperties
import scala.concurrent.Future

package object store {

  case object Done

  trait DBOpService extends {
    def insertMessage(id: Long, header: Option[BasicProperties], body: Option[Array[Byte]], exchange: String, routing: String, ttl: Option[Long]): Future[Unit]
    def selectMessage(id: Long): Future[Option[Message]]
    def deleteMessage(id: Long): Future[Unit]

    def insertQueueMeta(id: String, lastConsumed: Long, consumerCount: Int, isDurable: Boolean, queueMsgTtl: Option[Long]): Future[Unit]
    def insertQueueMsg(id: String, startOffset: Long, msgId: Long, size: Int, ttl: Option[Long]): Future[Unit]
    def deleteQueueMsgs(id: String): Future[Unit]
    def consumedQueueMessages(id: String, lastConsumed: Long, consumerCount: Int, isDurable: Boolean, queueMsgTtl: Option[Long], unacks: Vector[Long]): Future[Unit]
    def selectQueue(id: String): Future[Option[Queue]]
    def forceDeleteQueue(id: String): Future[Unit]
    def pendingDeleteQueue(id: String): Future[Unit]
    def deleteQueueConsumedMsgs(id: String): Future[Unit]
    def insertQueueUnacks(id: String, msgIds: List[Long]): Future[Unit]
    def deleteQueueUnack(id: String, msgId: Long): Future[Unit]

    def insertExchange(id: String, tpe: String, isDurable: Boolean, isAutoDelete: Boolean, isInternal: Boolean, args: java.util.Map[String, Any]): Future[Unit]
    def insertExchangeBind(id: String, queue: String, routingKey: String, args: java.util.Map[String, Any]): Future[Unit]
    def selectExchange(id: String): Future[Option[Exchange]]
    def deleteExchangeBind(id: String, queue: String, routingKey: String): Future[Unit]
    def deleteExchangeBindsOfQueue(id: String, queue: String): Future[Unit]
    def deleteExchange(id: String): Future[Unit]
  }
}
