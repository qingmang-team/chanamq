package chana.mq.amqp.server.entity

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
import chana.mq.amqp.model.AMQConsumer
import chana.mq.amqp.model.Msg
import chana.mq.amqp.model.Queue
import chana.mq.amqp.server.Loaded
import chana.mq.amqp.server.Unlock
import chana.mq.amqp.server.VHostCommand
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
    case cmd: VHostCommand => (cmd.entityId, cmd)
  }

  private val extractShardId: ShardRegion.ExtractShardId = {
    case cmd: VHostCommand => (cmd.entityId.hashCode % 100).toString
  }

  def startSharding(implicit system: ActorSystem) =
    ClusterSharding(system).start(typeName, props(), ClusterShardingSettings(system), extractEntityId, extractShardId)

  def startShardingProxy(implicit system: ActorSystem) =
    ClusterSharding(system).startProxy(typeName, None, extractEntityId, extractShardId)

  val Topic = "amqp.queue"

  final case class Statistics(existed: Boolean, queueSize: Int, consumerCount: Int)

  final case class Existed(vhost: String, id: String) extends VHostCommand
  final case class Declare(vhost: String, id: String, durable: Boolean, exclusive: Boolean, autoDelete: Boolean, connectionId: Int, ttl: Option[Long]) extends VHostCommand
  final case class ForceDelete(vhost: String, id: String, connectionId: Int) extends VHostCommand
  final case class PendingDelete(vhost: String, id: String, connectionId: Int) extends VHostCommand
  final case class Purge(vhost: String, id: String, connectionId: Int) extends VHostCommand

  final case class IsDurable(vhost: String, id: String) extends VHostCommand
  final case class Push(vhost: String, id: String, msgs: Vector[Msg], connectionId: Int) extends VHostCommand
  final case class Pull(vhost: String, id: String, consumerTag: Option[String], connectionId: Int, channelId: Int, prefetchCount: Int, prefetchSize: Long, autoAck: Boolean) extends VHostCommand
  final case class Acked(vhost: String, id: String, msgIds: collection.Set[Long]) extends VHostCommand
  final case class Requeue(vhost: String, id: String, msgIds: collection.Set[Long]) extends VHostCommand

  final case class ConsumerStarted(vhost: String, id: String, tag: String, connectionId: Int, channelId: Int) extends VHostCommand
  final case class ConsumerCancelled(vhost: String, id: String, tag: String, connectionId: Int, channelId: Int) extends VHostCommand

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

  private var messages = Vector[Msg]()
  private var lastConsumed = 0L

  private var unacks = Map[Long, Msg]()

  // TODO for case of connection suddenly boken on server side, we should check last active time of consumers to remove it
  private var consumerToLastActive = Map[String, Long]()

  private val mediator = DistributedPubSub(context.system).mediator

  private val serviceBoard = ServiceBoard(context.system)
  def storeService = serviceBoard.storeService

  private def messageSharding = ClusterSharding(context.system).shardRegion(MessageEntity.typeName)

  private def load(): Future[Unit] = {
    log.info(s"$id loading...")
    storeService.selectQueue(id) map {
      case Some(Queue(lastConsumed, consumers, isDurable, queueMsgTtl, msgs, unacks)) =>
        this.isPassiveCreated = false

        this.isDurable = isDurable
        this.lastConsumed = lastConsumed
        this.queueMsgTtl = queueMsgTtl
        this.messages = msgs
        this.unacks ++= unacks.map(x => x.id -> x)
        val now = System.currentTimeMillis
        this.consumerToLastActive = consumers.map(_ -> now).toMap

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
      log.info(s"$id loaded: messageCount=${messages.length}, lastConsumed=$lastConsumed, consumers=${consumerToLastActive.size}, isDurable=$isDurable, unacked=$unacks")
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
    case QueueEntity.Existed(_, _) =>
      sender() ! existed

    case QueueEntity.Declare(_, _, durable, exclusive, autoDelete, connectionId, ttl) =>
      val commander = sender()
      this.isPassiveCreated = false

      declare(durable, exclusive, autoDelete, connectionId, ttl) map { stats =>
        commander ! stats
      }

    case QueueEntity.IsDurable(_, _) =>
      sender() ! isDurable

    case QueueEntity.ForceDelete(_, _, connectionId) =>
      val commander = sender()

      delete(connectionId, isForce = true) andThen {
        case Success(x)  => commander ! x
        case Failure(ex) => log.error(ex, ex.getMessage)
      } map {
        case (true, _)  => self ! PoisonPill
        case (false, _) =>
      }

    case QueueEntity.PendingDelete(_, _, connectionId) =>
      val commander = sender()

      delete(connectionId, isForce = false) andThen {
        case Success(x)  => commander ! x
        case Failure(ex) => log.error(ex, ex.getMessage)
      } map {
        case (true, _)  => self ! PoisonPill
        case (false, _) =>
      }

    case m @ QueueEntity.Purge(_, _, connectionId) =>
      val commander = sender()

      if (isExclusive && this.connectionId != connectionId) {
        log.error(s"$m: access exclusive queue from another connection")
        commander ! (false, 0)
      } else {
        val nPurged = messages.length
        // removes all messages from a queue which are not awaiting ack -- we've kept in unacks
        messages = Vector()
        val persist = if (isDurable) {
          storeService.deleteQueueMsgs(id)
        } else {
          Future.successful(())
        }

        persist map { _ =>
          commander ! (true, nPurged)
        }
      }

    case m @ QueueEntity.ConsumerStarted(_, _, tag, connectionId, channelId) =>
      val commander = sender()

      if (isExclusive && this.connectionId != connectionId) {
        log.error(s"$m: access exclusive queue from another connection")
        commander ! false
      } else {
        consumerToLastActive += (AMQConsumer.globalId(tag, connectionId, channelId) -> System.currentTimeMillis)

        val persist = if (isDurable) {
          storeService.insertQueueMeta(id, lastConsumed, consumerToLastActive.keySet, isDurable, queueMsgTtl)
        } else {
          Future.successful(())
        }

        persist map { _ =>
          commander ! true
        }
      }

    case m @ QueueEntity.ConsumerCancelled(_, _, tag, connectionId, channelId) =>
      val commander = sender()

      if (isExclusive && this.connectionId != connectionId) {
        log.error(s"$m: access exclusive queue from another connection")
        commander ! false
      } else {
        consumerToLastActive -= AMQConsumer.globalId(tag, connectionId, channelId)

        if (isAutoDelete && consumerToLastActive.isEmpty) {
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
            storeService.insertQueueMeta(id, lastConsumed, consumerToLastActive.keySet, isDurable, queueMsgTtl)
          } else {
            Future.successful(())
          }

          persist map { _ =>
            commander ! true
          }
        }
      }

    case m @ QueueEntity.Push(_, _, msgs, connectionId) =>
      log.debug(s"Got $m")
      val commander = sender()

      if (isExclusive && this.connectionId != connectionId) {
        log.error(s"$m: access exclusive queue from another connection")
        commander ! false
      }

      log.debug(s"$id msgs pushed in: $msgs")

      val now = System.currentTimeMillis
      val newOffset = messages.lastOption match {
        case Some(last) => last.offset + 1
        case None       => lastConsumed + 1
      }

      val (_, offsetedMsgs) = msgs.foldLeft((newOffset, Vector[Msg]())) {
        case ((offset, offsetedMsgs), msg) =>
          queueMsgTtl match {
            case Some(ttl) =>
              val expireTime = msg.expireTime.map(math.min(_, now + ttl)) orElse Some(now + ttl)
              (offset + 1, offsetedMsgs :+ msg.copy(offset = offset, expireTime = expireTime))
            case None =>
              (offset + 1, offsetedMsgs :+ msg.copy(offset = offset))
          }
      }

      log.debug(s"$id: offsetedMsgs = $offsetedMsgs")

      messages ++= offsetedMsgs

      val persist = if (isDurable) {
        Future.sequence(offsetedMsgs map { msg =>
          storeService.insertQueueMsg(id, msg.offset, msg.id, msg.bodySize, msg.expireTime.map(_ - now))
        })
      } else {
        Future.successful(())
      }

      persist map { _ =>
        commander ! consumerToLastActive.nonEmpty // consumer non empty -> will be consumed immediatelly
      } onComplete {
        case Success(_) =>
        case Failure(e) => log.error(e, e.getMessage)
      }

    case m @ QueueEntity.Pull(_, _, consumerTag, connectionId, channelId, prefetchCount, prefetchSize, autoAck) =>
      log.debug(s"Got $m")
      val commander = sender()

      if (isExclusive && this.connectionId != connectionId) {
        log.error(s"$m: access exclusive queue from another connection")
        commander ! Vector()
      } else {

        consumerTag foreach { tag =>
          consumerToLastActive += (AMQConsumer.globalId(tag, connectionId, channelId) -> System.currentTimeMillis)
        }

        if (messages.nonEmpty) {
          val maxCount = math.min(messages.length, prefetchCount)
          log.debug(s"$id msgs: $messages, max fetching count: $maxCount")

          val now = System.currentTimeMillis
          // use LinkedHashMap to keep inserting order
          val msgIdToMsg = new mutable.LinkedHashMap[Long, Msg]()
          var count = 0
          var lastOffset = lastConsumed
          var msgsBodySize = 0
          val msgsItr = messages.iterator
          while (msgsItr.hasNext && count < maxCount && msgsBodySize < prefetchSize) {
            val msg = msgsItr.next()
            log.debug(s"now $now, msg expire time: ${msg.expireTime}")
            val expired = queueMsgTtl.isDefined && msg.expired(now)

            // we do not need to concern about deleting expired msg from queue's persistence, which will expire by self
            if (expired) {
              count += 1
              lastOffset = msg.offset
              messageSharding ! MessageEntity.Unrefer(msg.id.toString)
            } else {
              msgsBodySize += msg.bodySize
              if (msgsBodySize < prefetchSize) {
                count += 1
                lastOffset = msg.offset
                msgIdToMsg += (msg.id -> msg)
              }
            }
          }

          if (msgIdToMsg.nonEmpty) {
            lastConsumed = lastOffset
            messages = messages.drop(count)
            if (!autoAck) {
              unacks ++= msgIdToMsg
            }

            log.debug(s"$id msgs: $count $msgIdToMsg, lastConsumed: $lastConsumed, msgs left: ${messages.length}")

            val persist = if (isDurable) {
              if (autoAck) {
                storeService.consumedQueueMessages(id, lastConsumed, Vector())
              } else {
                storeService.consumedQueueMessages(id, lastConsumed, msgIdToMsg.values)
              }
            } else {
              Future.successful(())
            }

            persist map { _ =>
              commander ! msgIdToMsg.keys.toVector
            } onComplete {
              case Success(_) =>
              case Failure(e) => log.error(e, e.getMessage)
            }
          } else {
            commander ! Vector()
          }
        } else {
          commander ! Vector()
        }
      }

    case m @ QueueEntity.Acked(_, _, msgIds) =>
      log.debug(s"Got $m")
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

    case m @ QueueEntity.Requeue(_, _, msgIds) =>
      log.debug(s"Got $m")
      val commander = sender()

      val now = System.currentTimeMillis
      val msgs = msgIds.flatMap(unacks.get).toVector.sortBy(_.offset)
      messages = (msgs ++ messages).sortBy(_.offset)
      messages.headOption foreach { x =>
        lastConsumed = x.offset - 1
      }

      unacks --= msgIds

      Future.sequence(msgIds map { msgId =>
        if (isDurable) {
          Future.sequence(msgs map { msg =>
            storeService.insertQueueMsg(id, msg.offset, msg.id, msg.bodySize, msg.expireTime.map(_ - now))
          }) flatMap { _ =>
            storeService.insertLastConsumed(id, lastConsumed)
          } flatMap { _ =>
            storeService.deleteQueueUnack(id, msgId)
          }
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
      storeService.insertQueueMeta(id, lastConsumed, consumerToLastActive.keySet, isDurable, queueMsgTtl)
    } else {
      Future.successful(())
    }

    persist map { _ => QueueEntity.Statistics(existed, messages.length, consumerToLastActive.size) }
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

      persist map { _ => (true, messages.length) }
    }

  }
}
