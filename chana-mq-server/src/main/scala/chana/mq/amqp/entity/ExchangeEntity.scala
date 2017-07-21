package chana.mq.amqp.entity

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Stash
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import akka.cluster.pubsub.DistributedPubSubMediator.SubscribeAck
import akka.cluster.sharding.ClusterSharding
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.ShardRegion
import akka.pattern.ask
import akka.util.Timeout
import chana.mq.amqp.Loaded
import chana.mq.amqp.Command
import chana.mq.amqp.Exchange
import chana.mq.amqp.Msg
import chana.mq.amqp.method.Basic
import chana.mq.amqp.model.AMQP
import chana.mq.amqp.server.engine.DirectMatcher
import chana.mq.amqp.server.engine.FanoutMatcher
import chana.mq.amqp.server.engine.QueueMatcher
import chana.mq.amqp.server.engine.Subscriber
import chana.mq.amqp.server.engine.Subscription
import chana.mq.amqp.server.engine.TrieMatcher
import chana.mq.amqp.server.service.ServiceBoard
import chana.mq.amqp.server.store
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

/**
 * An exchange accepts messages from a producer application and routes these to message queues according
 * to pre-arranged criteria. These criteria are called "bindings". Exchanges are matching and routing engines.
 * That is, they inspect messages and using their binding tables, decide how to forward these messages to
 * message queues or other exchanges. Exchanges never store messages.
 *
 * The term "exchange" is used to mean both a class of algorithm, and the instances of such an algorithm.
 * More properly, we speak of the "exchange type" and the "exchange instance".
 *
 * AMQP defines a number of standard exchange types, which cover the fundamental types of routing needed
 * to do common message delivery. AMQP servers will provide default instances of these exchanges.
 * Applications that use AMQP can additionally create their own exchange instances. Exchange types are
 * named so that applications which create their own exchanges can tell the server what exchange type to use.
 * Exchange instances are also named so that applications can specify how to bind queues and publish
 * messages.
 *
 * Exchanges can do more than route messages. They can act as intelligent agents that work from within the
 * server, accepting messages and producing messages as needed. The exchange concept is intended to define
 * a model for adding extensibility to AMQP servers in a reasonably standard way, since extensibility has
 * some impact on interoperability.
 *
 * The AMQP 0.9.1 specification states that the binding of durable queues to transient exchanges must be
 * allowed. In this case, since the exchange would not survive a broker restart, neither would any
 * bindings to such and exchange.
 *
 * Durable exchanges survive broker restart and node added/removed, transient exchanges do not (they have
 * to be redeclared when the broker comes back online).
 */
object ExchangeEntity {
  def props() = Props(classOf[ExchangeEntity]).withDispatcher("chana-mq-exchange-dispatcher")

  val typeName: String = "exchangeEntity"

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

  val Topic = "amqp.exchange"

  final case class Existed(id: String) extends Command
  final case class Declare(id: String, tpe: String, durable: Boolean, autoDelete: Boolean, internal: Boolean, arguments: Map[String, Any]) extends Command
  final case class Delete(id: String, ifUnused: Boolean, connectionId: Int) extends Command
  final case class QueueBind(id: String, queue: String, routingKey: String, arguments: Map[String, Any]) extends Command
  final case class QueueUnbind(id: String, queue: String, routingKey: String, arguments: Map[String, Any]) extends Command
  final case class QueueUnbindAll(id: String, queue: String) extends Command
  final case class Publishs(id: String, publishes: List[Publish]) extends Command

  final case class Publish(msgId: Long, publish: Basic.Publish, bodySize: Int, isMsgPersist: Boolean, ttl: Option[Long])

  final case class TopicSubscriber(queue: String, isDurable: Boolean) extends Subscriber

  def isDefaultExchange(exchange: String) =
    exchange == null || exchange == ""

  def messagIdGenerator() = {
    System.currentTimeMillis << 22
  }

}
/**
 * Durability and related matters: http://rubybunny.info/articles/durability.html
 */
final class ExchangeEntity() extends Actor with Stash with ActorLogging {
  import context.dispatcher
  private implicit val timeout: Timeout = context.system.settings.config.getInt("chana.mq.internal.timeout").seconds

  private var tpe: AMQP.ExchangeType = _

  private var isDurable: Boolean = _
  private var isAutoDelete: Boolean = _
  private var isInternal: Boolean = _
  private var args: Map[String, Any] = _

  /**
   * Is it created by sharding access only?
   */
  private var isPassiveCreated: Boolean = true
  private def existed = !isPassiveCreated

  private val binds = new mutable.ListBuffer[ExchangeEntity.QueueBind]()
  private val subscriptions = new mutable.HashMap[String, mutable.ListBuffer[Subscription]]()
  private var queueMatcher: QueueMatcher = _

  private val mediator = DistributedPubSub(context.system).mediator
  mediator ! Subscribe(ExchangeEntity.Topic, self)

  private def queueSharding = ClusterSharding(context.system).shardRegion(QueueEntity.typeName)
  private def messageSharding = ClusterSharding(context.system).shardRegion(MessageEntity.typeName)

  private val serviceBoard = ServiceBoard(context.system)
  def storeService = serviceBoard.storeService

  private def load(): Future[Unit] = {
    log.info(s"$id loading...")
    storeService.selectExchange(id) flatMap {
      case Some(Exchange(tpe, isDurable, isAutoDelete, isInternal, args, binds)) =>
        this.isPassiveCreated = false

        this.tpe = AMQP.ExchangeType.typeOf(tpe)
        this.isDurable = isDurable
        this.isAutoDelete = isAutoDelete
        this.isInternal = isInternal
        this.args = args

        queueMatcher = this.tpe match {
          case AMQP.ExchangeType.DIRECT => new DirectMatcher()
          case AMQP.ExchangeType.FANOUT => new FanoutMatcher()
          case AMQP.ExchangeType.TOPIC  => new TrieMatcher()
          case _                        => new TrieMatcher()
        }

        Future.sequence(binds map queueBind) map (_ => ())

      case None =>
        Future.successful(())
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
      log.info(s"$id loaded: tpe=$tpe, isDurable=$isDurable, isAutoDelete=$isAutoDelete, isInternal=$isInternal, args=$args, binds=$binds")
    case _ =>
      stash()
  }

  def ready: Receive = {
    case SubscribeAck(Subscribe(ExchangeEntity.Topic, None, `self`)) =>
      log.info(s"Subscribed to ${ExchangeEntity.Topic}")

    case QueueEntity.QueueDeleted(queue) =>
      // message published via mediator's ExchangeEntity.Topic
      queueUnbind(queue)

    case ExchangeEntity.Existed(id) =>
      sender() ! existed

    case ExchangeEntity.Declare(id, tpe, durable, autoDelete, internal, arguments) =>
      val commander = sender()
      this.isPassiveCreated = false

      this.tpe = AMQP.ExchangeType.typeOf(tpe)
      this.isDurable = durable
      if (!existed) {
        this.isAutoDelete = autoDelete
      }
      this.isInternal = internal
      this.args = arguments

      // TODO header matcher ?
      queueMatcher = this.tpe match {
        case AMQP.ExchangeType.DIRECT => new DirectMatcher()
        case AMQP.ExchangeType.FANOUT => new FanoutMatcher()
        case AMQP.ExchangeType.TOPIC  => new TrieMatcher()
        case _                        => new TrieMatcher()
      }

      val persist = if (isDurable) {
        storeService.insertExchange(id, tpe, isDurable, isAutoDelete, isInternal, args.asJava)
      } else {
        log.info(s"$id Exchange created")
        Future.successful(())
      }

      persist map { _ =>
        commander ! true
      }

    case ExchangeEntity.Delete(_, ifUnused, connectionId) =>
      val commander = sender()
      if (ifUnused && subscriptions.nonEmpty) {
        commander ! false
      } else {
        Future.sequence(subscriptions.keys map { queue =>
          (queueSharding ? QueueEntity.ForceDelete(queue, connectionId)).mapTo[(Boolean, Int)]
        }) flatMap { xs =>
          val persist = if (isDurable) {
            storeService.deleteExchange(id)
          } else {
            Future.successful(())
          }

          persist map { _ =>
            subscriptions.clear()
            commander ! true
            self ! PoisonPill
          }
        }
      }

    case bind @ ExchangeEntity.QueueBind(_, queue, routingKey, arguments) =>
      val commander = sender()

      val persist = queueBind(bind) flatMap { _ =>
        if (isDurable) {
          storeService.insertExchangeBind(id, queue, routingKey, arguments.asJava)
        } else {
          Future.successful(())
        }
      }

      persist map { _ =>
        commander ! true
      }

    case ExchangeEntity.QueueUnbind(_, queue, routingKey, arguments) =>
      val commander = sender()

      queueUnbind(queue, routingKey) map {
        case true =>
          commander ! true
          self ! PoisonPill
        case false =>
          commander ! true
      }

    case ExchangeEntity.Publishs(_, publishes) =>
      val commander = sender()

      publish(publishes) map { deliverables =>
        commander ! deliverables
      }

    case _ =>
  }

  private def publish(publishes: List[ExchangeEntity.Publish]): Future[Vector[Boolean]] = {
    log.debug(s"publishes: $publishes")

    val now = System.currentTimeMillis
    var queueToMsgs = Map[String, Vector[Msg]]()
    var msgRefers = Vector[MessageEntity.Refer]()
    publishes foreach {
      case ExchangeEntity.Publish(msgId, Basic.Publish(_, exchange, routingKey, mandatory, immediate), bodySize, isMsgPersist, ttl) =>
        log.debug(s"matcher: ${queueMatcher}")
        val queues = queueMatcher.lookup(routingKey).asInstanceOf[collection.Set[ExchangeEntity.TopicSubscriber]]
        log.debug(s"subs of '${routingKey}': ${queues}")

        val isDurable = this.isDurable && isMsgPersist && queues.map(_.isDurable).foldLeft(false)(_ || _)
        msgRefers :+= MessageEntity.Refer(msgId.toString, queues.size, isDurable)
        queues foreach { queue =>
          queueToMsgs += (queue.queue -> (queueToMsgs.getOrElse(queue.queue, Vector()) :+ Msg(msgId, 0, bodySize, ttl.map(_ + now))))
        }
    }

    log.debug(s"queueToMsgs: $queueToMsgs")

    Future.sequence(msgRefers map { ref =>
      (messageSharding ? ref).mapTo[Boolean]
    }) flatMap { _ =>
      Future.sequence(queueToMsgs map {
        case (queue, msgs) => (queueSharding ? QueueEntity.Push(queue, msgs)).mapTo[Boolean]
      })
    } map (_ => msgRefers.map(_.count > 0))
  }

  private def queueBind(bind: ExchangeEntity.QueueBind): Future[Unit] = {
    binds += bind

    log.info(s"$id, tpe: $tpe, macther: $queueMatcher")
    (queueSharding ? QueueEntity.IsDurable(bind.queue)).mapTo[Boolean] map { isQueueDutable =>
      val sub = queueMatcher.subscribe(bind.routingKey, ExchangeEntity.TopicSubscriber(bind.queue, isQueueDutable))
      subscriptions.getOrElseUpdate(bind.queue, new mutable.ListBuffer()) += sub
      log.debug(s"matcher after '${bind.routingKey}' - '${bind.queue}': ${queueMatcher}")
    }
  }

  private def queueUnbind(queue: String): Future[_] = {
    val bindsToRemove = binds.filter(x => x.queue == queue)
    binds --= bindsToRemove

    subscriptions.get(queue) map { subs =>
      subs foreach queueMatcher.unsubscribe
      subscriptions -= queue
    }

    if (isAutoDelete && subscriptions.isEmpty) {
      val persist = if (isDurable) {
        storeService.deleteExchange(id)
      } else {
        Future.successful(())
      }
      persist map { _ =>
        self ! PoisonPill
      }
    } else {
      storeService.deleteExchangeBindsOfQueue(id, queue)
    }
  }

  /**
   * return should kill self or not
   */
  private def queueUnbind(queue: String, routingKey: String): Future[Boolean] = {
    val bindsToRemove = binds.filter { bind => bind.queue == queue && bind.routingKey == routingKey }
    binds --= bindsToRemove

    log.info(s"$id, tpe: $tpe, macther: $queueMatcher")
    subscriptions.get(queue) map { subs =>
      subs find {
        case Subscription(`routingKey`, ExchangeEntity.TopicSubscriber(`queue`, _)) => true
        case _ => false
      } foreach { sub =>
        queueMatcher.unsubscribe(sub)
        subs -= sub
      }
    }
    log.debug(s"matcher after '${routingKey}' - '${queue}': ${queueMatcher}")

    val persist = if (isDurable) {
      storeService.deleteExchangeBind(id, queue, routingKey)
    } else {
      Future.successful(())
    }

    persist flatMap { _ =>
      // If autoDelete set, the exchange is deleted when all queues have finished using it.
      // The server SHOULD allow for a reasonable delay between the point when it 
      // determines that an exchange is not being used (or no longer used), and 
      // the point when it deletes the exchange. At the least it must allow a 
      // client to create an exchange and then bind a queue to it, with a small 
      // but non-zero delay between these two actions.
      if (isAutoDelete && subscriptions.isEmpty) {
        if (isDurable) {
          storeService.deleteExchange(id) map (_ => true)
        } else {
          Future.successful(true)
        }
      } else {
        Future.successful(false)
      }
    }
  }
}