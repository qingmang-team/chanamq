package chana.mq.amqp.server.service

import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.cluster.Cluster
import akka.pattern.ask
import chana.mq.amqp.server.store
import chana.mq.amqp.server.store.cassandra.CassandraOpService
import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * We'd like to use the name ServiceBoard instead of ServiceBoardExternsion,
 * so we can reach it via ServiceBoard(system) instead of longer name ServiceBoardExternsion(system)
 */
object ServiceBoard extends ExtensionId[ServiceBoardExtension] with ExtensionIdProvider {
  override def lookup = ServiceBoard
  override def createExtension(system: ExtendedActorSystem) = new ServiceBoardExtension(system)
}
class ServiceBoardExtension(system: ExtendedActorSystem) extends Extension {
  private val config = system.settings.config.getConfig("chana.mq.service-board")
  private val role: Option[String] = config.getString("role") match {
    case "" => None
    case r  => Some(r)
  }

  /**
   * Returns true if this member is not tagged with the role configured for the mediator.
   */
  def isTerminated: Boolean = Cluster(system).isTerminated || !role.forall(Cluster(system).selfRoles.contains)

  def nodeIdService = GlobalNodeIdService.proxy(system)

  lazy val idService = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val f = (nodeIdService ? GlobalNodeIdService.AskNodeId(UUID.randomUUID().toString))(20.seconds).mapTo[Int] map { nodeId =>
      new IdGenerator(nodeId)(system)
    }

    Await.result(f, 20.seconds)
  }

  lazy val storeService: store.DBOpService = new CassandraOpService(system)
}
