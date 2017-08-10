package chana.mq.amqp.server.entity

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Timers
import akka.cluster.sharding.ClusterSharding
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.ShardRegion
import chana.mq.amqp.model.BasicProperties
import chana.mq.amqp.model.Message
import chana.mq.amqp.server.ActiveCheckTick
import chana.mq.amqp.server.ActiveCheckTickKey
import chana.mq.amqp.server.Command
import chana.mq.amqp.server.service.ServiceBoard
import chana.mq.amqp.server.store
import java.time.LocalDateTime
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

/**
 * Each actor instance occupies about extra ~300 bytes, 10k actors will be ~2.86M
 * 1 millis will be ~286M, 100 millis will be 28G
 *
 * Message entitis keep in memory when refer count > 0, and will exit from memory
 * when refer count == 0 or inactive more than
 */
object MessageEntity {
  def props() = Props(classOf[MessageEntity])

  val typeName: String = "messageEntity"

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

  val Topic = "amqp.message"

  private case object ExpiredTickKey
  private case object ExpiredTick

  final case class Received(id: String, header: Option[BasicProperties], body: Option[Array[Byte]], exchange: String, routing: String, ttl: Option[Long]) extends Command
  final case class Get(id: String) extends Command
  final case class Refer(id: String, count: Int, isPersistent: Boolean) extends Command
  final case class Unrefer(id: String) extends Command

}

final class MessageEntity extends Actor with Timers with ActorLogging {
  import MessageEntity._
  import context.dispatcher

  private val inactiveInterval = context.system.settings.config.getInt("chana.mq.message.inactive")

  private val serviceBoard = ServiceBoard(context.system)
  def storeService = serviceBoard.storeService

  private var message: Message = _
  private var isPersistent = false

  private var referCount = 0
  private var isSavingOrSaved = false

  private var lastActiveTime = LocalDateTime.now()

  private val loading = Promise[Unit]
  private lazy val load: Future[Unit] = {
    storeService.selectMessage(longId) map {
      case Some((msg, isDurable, referCount)) =>
        this.message = msg
        this.isPersistent = isDurable
        this.referCount = referCount
        this.isSavingOrSaved = true
        setTTL(msg.ttl)

      case None =>
    } andThen {
      case Success(_) =>
      case Failure(e) => log.error(e, e.getMessage)
    }
  }
  private def promiseLoad(): Future[Unit] = {
    if (!loading.isCompleted) {
      loading tryCompleteWith load
    }
    loading.future
  }

  private lazy val longId = self.path.name.toLong

  override def postStop() {
    log.info(s"$longId stopped")
    super.postStop()
  }

  def receive = ready

  def ready: Receive = {
    case MessageEntity.Received(_, header, body, exchange, routing, ttl) =>
      val commander = sender()
      lastActiveTime = LocalDateTime.now()

      message = Message(longId, header, body, exchange, routing, ttl)
      isSavingOrSaved = false
      setTTL(ttl)

      loading.trySuccess(())

      commander ! true

    case MessageEntity.Get(_) =>
      val commander = sender()
      lastActiveTime = LocalDateTime.now()

      promiseLoad() map { _ =>
        commander ! Option(message)
      }

    case MessageEntity.Refer(_, count, isPersistent) =>
      val commander = sender()
      lastActiveTime = LocalDateTime.now()

      this.isPersistent = isPersistent
      this.referCount += count
      log.debug(s"referCount after Referred $referCount")

      val persist = if (isPersistent && !isSavingOrSaved && message != null) {
        isSavingOrSaved = true
        storeService.insertMessage(longId, message.header, message.body, message.exchange, message.routingKey, isPersistent, referCount, message.ttl)
      } else {
        Future.successful(())
      }

      persist map { _ =>
        commander ! true
      }

    case MessageEntity.Unrefer(_) =>
      referCount -= 1
      log.debug(s"referCount after queue pull $referCount")

      if (referCount <= 0) {
        if (isSavingOrSaved) {
          storeService.deleteMessage(longId)
        }
        self ! PoisonPill
      } else {
        if (isPersistent) {
          storeService.updateMessageReferCount(longId, referCount)
        }
      }

    case ExpiredTick =>
      if (isSavingOrSaved) {
        storeService.deleteMessage(longId)
      }
      self ! PoisonPill

    case ActiveCheckTick =>
      if (LocalDateTime.now().minusSeconds(inactiveInterval).isAfter(lastActiveTime)) {
        val persist = if (!isSavingOrSaved && message != null) {
          isSavingOrSaved = true
          storeService.insertMessage(longId, message.header, message.body, message.exchange, message.routingKey, isPersistent, referCount, message.ttl)
        } else {
          Future.successful(())
        }

        persist map { _ =>
          self ! PoisonPill
        }
      }
  }

  private def setTTL(ttl: Option[Long]) {
    log.debug(s"ttl: $ttl")
    ttl match {
      case Some(x) =>
        timers.startSingleTimer(ExpiredTickKey, ExpiredTick, x.millis)
        timers.startPeriodicTimer(ActiveCheckTickKey, ActiveCheckTick, 5.minutes)
      case None =>
        timers.startPeriodicTimer(ActiveCheckTickKey, ActiveCheckTick, 5.minutes)
    }
  }

}