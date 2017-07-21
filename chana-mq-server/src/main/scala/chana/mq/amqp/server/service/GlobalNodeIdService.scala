package chana.mq.amqp.server.service

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.cluster.client.ClusterClientReceptionist
import akka.cluster.singleton.ClusterSingletonManager
import akka.cluster.singleton.ClusterSingletonManagerSettings
import akka.cluster.singleton.ClusterSingletonProxy
import akka.cluster.singleton.ClusterSingletonProxySettings

object GlobalNodeIdService {
  def props(): Props = Props(classOf[GlobalNodeIdService])

  val name = "nodeIdGenerator"
  val managerName = "chanaSingleton-" + name
  val managerPath = "/user/" + managerName
  val path = managerPath + "/" + name
  val proxyName = "chanaSingletonProxy-" + name
  val proxyPath = "/user/" + proxyName

  def start(system: ActorSystem, role: Option[String]): ActorRef = {
    val settings = ClusterSingletonManagerSettings(system).withRole(role).withSingletonName(name)
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = props(),
        terminationMessage = PoisonPill,
        settings = settings
      ), name = managerName
    )
  }

  def startProxy(system: ActorSystem, role: Option[String]): ActorRef = {
    val settings = ClusterSingletonProxySettings(system).withRole(role).withSingletonName(name)
    val proxy = system.actorOf(
      ClusterSingletonProxy.props(
        singletonManagerPath = managerPath,
        settings = settings
      ), name = proxyName
    )
    ClusterClientReceptionist(system).registerService(proxy)
    proxy
  }

  def proxy(system: ActorSystem) = system.actorSelection(proxyPath)

  final case class AskNodeId(name: String)
}

class GlobalNodeIdService extends Actor with ActorLogging {
  private var lastId = 0
  private var nameToId = Map[String, Int]()

  def receive = {
    case GlobalNodeIdService.AskNodeId(name) =>
      val id = nameToId.get(name) match {
        case Some(x) => x
        case None =>
          lastId += 1
          nameToId += (name -> lastId)
          lastId
      }
      sender() ! id
  }
}
