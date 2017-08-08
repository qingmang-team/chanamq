package chana.mq.amqp.server

import chana.mq.amqp.model.BasicProperties
import chana.mq.amqp.model.Exchange
import chana.mq.amqp.model.Message
import chana.mq.amqp.model.Msg
import chana.mq.amqp.model.Queue
import chana.mq.amqp.model.VirtualHost
import scala.concurrent.Future

package object store {

  case object Done

  trait DBOpService extends {
    def insertMessage(id: Long, header: Option[BasicProperties], body: Option[Array[Byte]], exchange: String, routing: String, isDurabale: Boolean, referCount: Int, ttl: Option[Long]): Future[Unit]
    def updateMessageReferCount(id: Long, referCount: Int): Future[Unit]
    def selectMessage(id: Long): Future[Option[(Message, Boolean, Int)]]
    def deleteMessage(id: Long): Future[Unit]

    def insertQueueMeta(id: String, lastConsumed: Long, consumers: Set[String], isDurable: Boolean, queueMsgTtl: Option[Long]): Future[Unit]
    def insertQueueMsg(id: String, startOffset: Long, msgId: Long, size: Int, ttl: Option[Long]): Future[Unit]
    def deleteQueueMsgs(id: String): Future[Unit]
    def insertLastConsumed(id: String, lastConsumed: Long): Future[Unit]
    def consumedQueueMessages(id: String, lastConsumed: Long, unacks: Iterable[Msg]): Future[Unit]
    def selectQueue(id: String): Future[Option[Queue]]
    def forceDeleteQueue(id: String): Future[Unit]
    def pendingDeleteQueue(id: String): Future[Unit]
    def deleteQueueConsumedMsgs(id: String): Future[Unit]
    def insertQueueUnack(id: String, offset: Long, msgId: Long, size: Int, ttl: Option[Long]): Future[Unit]
    def deleteQueueUnack(id: String, msgId: Long): Future[Unit]

    def insertExchange(id: String, tpe: String, isDurable: Boolean, isAutoDelete: Boolean, isInternal: Boolean, args: java.util.Map[String, Any]): Future[Unit]
    def insertExchangeBind(id: String, queue: String, routingKey: String, args: java.util.Map[String, Any]): Future[Unit]
    def selectExchange(id: String): Future[Option[Exchange]]
    def deleteExchangeBind(id: String, queue: String, routingKey: String): Future[Unit]
    def deleteExchangeBindsOfQueue(id: String, queue: String): Future[Unit]
    def deleteExchange(id: String): Future[Unit]

    def insertVhost(id: String, isActive: Boolean): Future[Unit]
    def selectVhost(id: String): Future[Option[VirtualHost]]
    def deleteVhost(id: String): Future[Unit]
  }
}
