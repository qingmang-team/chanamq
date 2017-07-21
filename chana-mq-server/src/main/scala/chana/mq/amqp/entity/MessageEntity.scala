package chana.mq.amqp.entity

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Cancellable
import akka.actor.PoisonPill
import akka.actor.Props
import akka.cluster.sharding.ClusterSharding
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.ShardRegion
import chana.mq.amqp.ActiveCheckTick
import chana.mq.amqp.Command
import chana.mq.amqp.Message
import chana.mq.amqp.model.BasicProperties
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

  final case object Expired

  final case class Received(id: String, header: Option[BasicProperties], body: Option[Array[Byte]], exchange: String, routing: String, ttl: Option[Long]) extends Command
  final case class Get(id: String) extends Command
  final case class Refer(id: String, count: Int, isDurable: Boolean) extends Command
  final case class Unrefer(id: String) extends Command

}

final class MessageEntity extends Actor with ActorLogging {
  import MessageEntity._
  import context.dispatcher

  private val inactiveInterval = context.system.settings.config.getInt("chana.mq.message.inactive")

  private val serviceBoard = ServiceBoard(context.system)
  def storeService = serviceBoard.storeService

  private var message: Message = _

  private var referCount = 0
  private var isSavingOrSaved = false

  private var lastActiveTime = LocalDateTime.now()
  private var activeCheckTask: Option[Cancellable] = Some(context.system.scheduler.schedule(5.minute, 5.minute, self, ActiveCheckTick))

  private val loading = Promise[Unit]
  private lazy val load: Future[Unit] = {
    storeService.selectMessage(longId) map {
      case Some(msg) =>
        this.message = msg
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

  private def longId = self.path.name.toLong

  override def postStop() {
    activeCheckTask.foreach(_.cancel)
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

    case MessageEntity.Refer(_, count, isDurable) =>
      val commander = sender()
      lastActiveTime = LocalDateTime.now()

      referCount += count
      log.debug(s"referCount after Referred $referCount")

      val persist = if (isDurable && !isSavingOrSaved && message != null) {
        isSavingOrSaved = true
        storeService.insertMessage(longId, message.header, message.body, message.exchange, message.routingKey, message.ttl)
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
        self ! PoisonPill
      }

    case Expired =>
      self ! PoisonPill

    case ActiveCheckTick =>
      if (LocalDateTime.now().minusSeconds(inactiveInterval).isAfter(lastActiveTime)) {
        self ! PoisonPill
      }
  }

  private def setTTL(ttl: Option[Long]) {
    log.debug(s"ttl: $ttl")
    activeCheckTask = ttl match {
      case Some(x) => Some(context.system.scheduler.scheduleOnce(x.millis, self, Expired))
      case None    => Some(context.system.scheduler.schedule(5.minutes, 5.minutes, self, ActiveCheckTick))
    }
  }

}