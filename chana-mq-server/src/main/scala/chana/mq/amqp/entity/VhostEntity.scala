package chana.mq.amqp.entity

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Stash
import akka.cluster.sharding.ClusterSharding
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.ShardRegion
import chana.mq.amqp.Command
import chana.mq.amqp.Loaded
import chana.mq.amqp.model.VirtualHost
import chana.mq.amqp.server.service.ServiceBoard
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

object VhostEntity {
  def props() = Props(classOf[VhostEntity])

  val typeName: String = "vhostEntity"

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

  val Topic = "amqp.vhost"

  final case class Existed(id: String) extends Command
  final case class Create(id: String) extends Command
  final case class Delete(id: String) extends Command
  final case class Active(id: String, isActive: Boolean) extends Command
}

final class VhostEntity extends Actor with Stash with ActorLogging {
  import context.dispatcher

  /**
   * Is it created by sharding access only?
   */
  private var isPassiveCreated: Boolean = true
  private def existed = !isPassiveCreated || (id == VirtualHost.defaultId)

  private var isActive: Boolean = true

  private val serviceBoard = ServiceBoard(context.system)
  def storeService = serviceBoard.storeService

  private def load(): Future[Unit] = {
    log.info(s"$id loading...")
    storeService.selectVhost(id) map {
      case Some(VirtualHost(id, isActive)) =>
        this.isPassiveCreated = false

        this.isActive = isActive
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

  override def postStop() {
    log.info(s"$id stopped")
    super.postStop()
  }

  def receive = initialize

  def initialize: Receive = {
    case Loaded =>
      context.become(ready)
      unstashAll()
      log.info(s"$id loaded: existed=${existed} isActive=${isActive}")
    case _ =>
      stash()
  }

  def ready: Receive = {
    case VhostEntity.Existed(_) =>
      sender() ! (if (existed) Some(VirtualHost(id, isActive)) else None)

    case VhostEntity.Create(_) =>
      val commander = sender()
      this.isPassiveCreated = false

      storeService.insertVhost(id, isActive) map { _ =>
        commander ! Some(VirtualHost(id, isActive))
      }

    case VhostEntity.Delete(_) =>
      val commander = sender()

      // TODO delete all exchanges/queues/binds under this vhost
      storeService.deleteVhost(id) map { _ =>
        commander ! true
        self ! PoisonPill
      }

    case VhostEntity.Active(_, isActive) =>
      val commander = sender()

      this.isActive = isActive
      storeService.insertVhost(id, isActive) map { _ =>
        commander ! true
      }

  }

}
