package chana.mq.amqp.entity

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Stash
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import akka.cluster.sharding.ClusterSharding
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.ShardRegion
import akka.pattern.ask
import akka.util.Timeout
import chana.mq.amqp
import chana.mq.amqp.Command
import chana.mq.amqp.Loaded
import chana.mq.amqp.Unlock
import chana.mq.amqp.Msg
import chana.mq.amqp.Queue
import chana.mq.amqp.server.service.ServiceBoard
import chana.mq.amqp.server.store
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

/**
 * Durable queues survive broker restart and node added/removed, transient queues
 * do not (they have to be redeclared when the broker comes back online).
 */
object QueueEntity {
  def props() = Props(classOf[QueueEntity])

  val typeName: String = "queueEntity"

  private val extractEntityId: ShardRegion.ExtractEntityId = {
    case cmd: Command => (cmd.id, cmd)
  }

  private val extractShardId: ShardRegion.ExtractShardId = {
    case cmd: Command => (cmd.id.hashCode % 100).toString
  }

  def startSharding(implicit system: ActorSystem) =
    ClusterSharding(system).start(typeName, props(), ClusterShardingSettings(system), extractEntityId, extractShardId)

  def startShardingProxy(implicit system: ActorSystem) =
    ClusterSharding(system).startProxy(typeName, None, extractEntityId, extractShardId)

  val Topic = "amqp.queue"

  final case class Statistics(existed: Boolean, queueSize: Int, consumerCount: Int)

  final case class Existed(id: String) extends Command
  final case class Declare(id: String, durable: Boolean, exclusive: Boolean, autoDelete: Boolean, connectionId: Int, ttl: Option[Long]) extends Command
  final case class ForceDelete(id: String, connectionId: Int) extends Command
  final case class PendingDelete(id: String, connectionId: Int) extends Command
  final case class Purge(id: String, connectionId: Int) extends Command

  final case class IsDurable(id: String) extends Command
  final case class Push(id: String, msgs: Vector[Msg]) extends Command
  final case class Pull(id: String, prefetchCount: Int, prefetchSize: Long, autoAck: Boolean) extends Command
  final case class Acked(id: String, msgId: List[Long]) extends Command

  final case class ConsumerStarted(id: String, connectionId: Int) extends Command
  final case class ConsumerCancelled(id: String, connectionId: Int) extends Command

  /** Event publish to "amqp.queue" */
  final case class QueueDeleted(queue: String)
}

final class QueueEntity extends Actor with Stash with ActorLogging {
  import context.dispatcher
  private implicit val timeout: Timeout = context.system.settings.config.getInt("chana.mq.internal.timeout").seconds

  /**
   * Is it created by sharding access only?
   */
  private var isPassiveCreated: Boolean = true
  private def existed = !isPassiveCreated

  private var isDurable = false
  private var isExclusive = false
  private var isAutoDelete = false
  private var connectionId = 0
  private var queueMsgTtl: Option[Long] = None

  private var messageIds = Vector[Msg]()
  private var lastConsumed = 0L

  private var unacks = Set[Long]()

  private var consumerCount: Int = _

  private val mediator = DistributedPubSub(context.system).mediator

  private val serviceBoard = ServiceBoard(context.system)
  def storeService = serviceBoard.storeService

  private def messageSharding = ClusterSharding(context.system).shardRegion(MessageEntity.typeName)

  private def load(): Future[Unit] = {
    log.info(s"$id loading...")
    storeService.selectQueue(id) map {
      case Some(Queue(lastConsumed, consumerCount, isDurable, queueMsgTtl, msgs, unacks)) =>
        this.isPassiveCreated = false

        //this.consumerCount = consumerCount // We can not keep consistence of consumerCount during a server restart?
        this.isDurable = isDurable
        this.lastConsumed = lastConsumed
        this.queueMsgTtl = queueMsgTtl
        this.messageIds = msgs
        this.unacks ++= unacks

      case None =>
    } andThen {
      case Success(_) =>
      case Failure(e) => log.error(e, e.getMessage)
    }
  }

  private def id = self.path.name

  override def preStart() {
    super.preStart()
    load() map { _ =>
      self ! Loaded
    }
  }

  def receive = initialize

  def initialize: Receive = {
    case Loaded =>
      context.become(ready)
      unstashAll()
      log.info(s"$id loaded: messageCount=${messageIds.length}, lastConsumed=$lastConsumed, consumerCount=$consumerCount, isDurable=$isDurable, unacked=$unacks")
    case _ =>
      stash()
  }

  private def lock() { context.become(locked) }
  private def unlock { self ! Unlock }
  def locked: Receive = {
    case Unlock =>
      context.become(ready)
      unstashAll()
    case _ =>
      stash()
  }

  def ready: Receive = {
    case QueueEntity.Existed(_) =>
      sender() ! existed

    case QueueEntity.Declare(_, durable, exclusive, autoDelete, connectionId, ttl) =>
      val commander = sender()
      this.isPassiveCreated = false

      declare(durable, exclusive, autoDelete, connectionId, ttl) map { stats =>
        commander ! stats
      }

    case QueueEntity.IsDurable(_) =>
      sender() ! isDurable

    case QueueEntity.ForceDelete(_, connectionId) =>
      val commander = sender()

      delete(connectionId, isForce = true) andThen {
        case Success(x)  => commander ! x
        case Failure(ex) => log.error(ex, ex.getMessage)
      } map {
        case (true, _)  => self ! PoisonPill
        case (false, _) =>
      }

    case QueueEntity.PendingDelete(_, connectionId) =>
      val commander = sender()

      delete(connectionId, isForce = false) andThen {
        case Success(x)  => commander ! x
        case Failure(ex) => log.error(ex, ex.getMessage)
      } map {
        case (true, _)  => self ! PoisonPill
        case (false, _) =>
      }

    case QueueEntity.Purge(_, connectionId) =>
      val commander = sender()

      if (isExclusive && this.connectionId != connectionId) {
        commander ! (false, 0)
      } else {
        val nPurged = messageIds.length
        // removes all messages from a queue which are not awaiting ack -- we've kept in unacks
        messageIds = Vector()
        val persist = if (isDurable) {
          storeService.deleteQueueMsgs(id)
        } else {
          Future.successful(())
        }

        persist map { _ =>
          commander ! (true, nPurged)
        }
      }

    case QueueEntity.ConsumerStarted(_, connectionId) =>
      val commander = sender()

      if (isExclusive && this.connectionId != connectionId) {
        commander ! false
      } else {
        consumerCount += 1

        val persist = if (isDurable) {
          storeService.insertQueueMeta(id, lastConsumed, consumerCount, isDurable, queueMsgTtl)
        } else {
          Future.successful(())
        }

        persist map { _ =>
          commander ! true
        }
      }

    case QueueEntity.ConsumerCancelled(_, connectionId) =>
      val commander = sender()

      if (isExclusive && this.connectionId != connectionId) {
        commander ! false
      } else {
        consumerCount -= 1

        if (isAutoDelete && consumerCount == 0) {
          mediator ! Publish(ExchangeEntity.Topic, QueueEntity.QueueDeleted(id))

          val persist = if (isDurable) {
            storeService.forceDeleteQueue(id)
          } else {
            Future.successful(())
          }

          persist map { _ =>
            commander ! true
            self ! PoisonPill
          }
        } else {
          val persist = if (isDurable) {
            storeService.insertQueueMeta(id, lastConsumed, consumerCount, isDurable, queueMsgTtl)
          } else {
            Future.successful(())
          }

          persist map { _ =>
            commander ! true
          }
        }
      }

    case QueueEntity.Push(_, msgs) =>
      val commander = sender()

      log.debug(s"$id msgs pushed in: $msgs")

      val now = System.currentTimeMillis
      val newOffset = messageIds.lastOption match {
        case Some(last) => last.offset + 1
        case None       => lastConsumed + 1
      }

      val (_, offsetedMsgs) = msgs.foldLeft((newOffset, Vector[Msg]())) {
        case ((offset, offsetedMsgs), msg) =>
          queueMsgTtl match {
            case Some(x) =>
              val expireTime = msg.expireTime map (math.min(_, now + x)) orElse Some(now + x)
              (offset + 1, offsetedMsgs :+ msg.copy(offset = offset, expireTime = expireTime))
            case None =>
              (offset + 1, offsetedMsgs :+ msg.copy(offset = offset))
          }
      }

      log.debug(s"$id: offsetedMsgs = $offsetedMsgs")

      messageIds ++= offsetedMsgs

      val persist = if (isDurable) {
        Future.sequence(offsetedMsgs map { msg =>
          storeService.insertQueueMsg(id, msg.offset, msg.id, msg.bodySize, msg.expireTime.map(_ - now))
        })
      } else {
        Future.successful(())
      }

      persist map { _ =>
        commander ! isDurable
      } onComplete {
        case Success(_) =>
        case Failure(e) =>
          log.error(e, e.getMessage)
      }

    case QueueEntity.Pull(_, prefetchCount, prefetchSize, autoAck) =>
      val commander = sender()

      if (messageIds.nonEmpty) {
        val maxCount = math.min(messageIds.length, prefetchCount)
        log.debug(s"$id msgs: $messageIds, max fetching count: $maxCount")

        val now = System.currentTimeMillis
        var msgIds = Vector[Long]()
        var count = 0
        var lastOffset = lastConsumed
        var msgsBodySize = 0
        val msgsItr = messageIds.iterator
        while (msgsItr.hasNext && count < maxCount && msgsBodySize < prefetchSize) {
          val msg = msgsItr.next()
          val expired = queueMsgTtl.isDefined && msg.expired(now)

          if (expired) {
            count += 1
            lastOffset = msg.offset
            messageSharding ! MessageEntity.Unrefer(msg.id.toString)
          } else {
            msgsBodySize += msg.bodySize
            if (msgsBodySize < prefetchSize) {
              count += 1
              lastOffset = msg.offset
              msgIds :+= msg.id
            }
          }
        }
        (msgIds, count)

        log.debug(s"$id msgIds: $count ${msgIds.mkString("(", ",", ")")}, lastConsumed: $lastConsumed, msgs left: ${messageIds.length}")

        lastConsumed = lastOffset
        messageIds = messageIds.drop(count)

        if (msgIds.nonEmpty) {
          if (!autoAck) {
            unacks ++= msgIds
          }

          val persist = if (isDurable) {
            if (autoAck) {
              storeService.consumedQueueMessages(id, lastConsumed, consumerCount, isDurable, queueMsgTtl, Vector())
            } else {
              storeService.consumedQueueMessages(id, lastConsumed, consumerCount, isDurable, queueMsgTtl, msgIds)
            }
          } else {
            Future.successful(())
          }

          persist map { _ =>
            commander ! msgIds
          } onComplete {
            case Success(_) =>
            case Failure(e) =>
              log.error(e, e.getMessage)
          }
        } else {
          commander ! Vector()
        }
      } else {
        commander ! Vector()
      }

    case QueueEntity.Acked(_, msgIds) =>
      val commander = sender()

      unacks --= msgIds

      Future.sequence(msgIds map { msgId =>
        if (isDurable) {
          storeService.deleteQueueUnack(id, msgId)
        } else {
          Future.successful(())
        }
      }) onComplete {
        case Success(_) =>
          commander ! true
        case Failure(ex) =>
          commander ! false
          log.error(ex, ex.getMessage)
      }
  }

  private def declare(durable: Boolean, exclusive: Boolean, autoDelete: Boolean, connectionId: Int, ttl: Option[Long]): Future[QueueEntity.Statistics] = {
    this.isDurable = durable
    this.isExclusive = exclusive
    this.isAutoDelete = autoDelete
    this.queueMsgTtl = ttl

    if (exclusive) {
      this.connectionId = connectionId
    }

    val persist = if (isDurable) {
      storeService.insertQueueMeta(id, lastConsumed, consumerCount, isDurable, queueMsgTtl)
    } else {
      Future.successful(())
    }

    persist map { _ => QueueEntity.Statistics(existed, messageIds.length, consumerCount) }
  }

  private def delete(connectionId: Int, isForce: Boolean): Future[(Boolean, Int)] = {
    if (isExclusive && this.connectionId != connectionId) {
      Future.successful((false, 0))
    } else {
      mediator ! Publish(ExchangeEntity.Topic, QueueEntity.QueueDeleted(id))

      val persist = if (isDurable) {
        if (isForce) {
          storeService.forceDeleteQueue(id)
        } else {
          storeService.pendingDeleteQueue(id)
        }
      } else {
        Future.successful(())
      }

      persist map { _ => (true, messageIds.length) }
    }

  }
}
