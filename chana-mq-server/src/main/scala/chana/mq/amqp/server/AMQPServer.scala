package chana.mq.amqp.server

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.ActorMaterializer
import akka.stream.FlowShape
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.MergePreferred
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Tcp
import akka.util.ByteString
import chana.mq.amqp.entity.ExchangeEntity
import chana.mq.amqp.entity.MessageEntity
import chana.mq.amqp.entity.QueueEntity
import chana.mq.amqp.model.Frame
import chana.mq.amqp.server.engine.FrameStage
import chana.mq.amqp.server.engine.TcpStage
import chana.mq.amqp.server.service.GlobalNodeIdService
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

object AMQPServer {

  implicit val system = ActorSystem("chanamq")
  implicit val materializer = ActorMaterializer()

  val log = Logging(system, this.getClass)

  GlobalNodeIdService.start(system, Some("entity"))
  GlobalNodeIdService.startProxy(system, Some("entity"))

  ExchangeEntity.startSharding(system)
  QueueEntity.startSharding(system)
  MessageEntity.startSharding(system)

  def main(args: Array[String]) {
    // raw tcp stream
    // Flow[ByteString, ByteString, Future[OutgoingConnection]]

    val serverConfig = system.settings.config.getConfig("chana.mq.server")

    val host = serverConfig.getString("interface")
    val port = serverConfig.getInt("port")

    /**
     *
     *          +------------+  +--------------+
     * tcpIn   ->            |  |              -> tcpOut
     *          |  merge     -> | frameHandler |
     * control ->            |  |              |
     *          +------------|  +--------------+
     *
     */
    def serverLogic()(implicit system: ActorSystem): Flow[ByteString, ByteString, NotUsed] =
      Flow.fromGraph(GraphDSL.create() { implicit builder =>
        import GraphDSL.Implicits._

        val control = builder.add(Source.tick(0.seconds, 1.micros, Frame.TICK_FRAME_BODY).buffer(1, OverflowStrategy.dropNew))
        val tcpIncoming = builder.add(Flow[ByteString])
        val tcpStage = builder.add(new TcpStage())
        val frameStage = builder.add(FrameStage.createStage())

        val merge = builder.add(MergePreferred[ByteString](1))
        tcpIncoming ~> tcpStage ~> merge.preferred
        control ~> merge.in(0)
        merge ~> frameStage.in

        FlowShape(tcpIncoming.in, frameStage.out)
      })

    val connectionSink = Sink.foreach[Tcp.IncomingConnection] { conn =>
      log.info(s"Incoming connection from: ${conn.remoteAddress}")
      conn.handleWith(serverLogic())
    }

    val binding: Future[Tcp.ServerBinding] = Tcp().bind(host, port).to(connectionSink).run()

    import system.dispatcher
    binding onComplete {
      case Success(b) =>
        log.info(s"Server started, listening on: ${b.localAddress}")
      case Failure(e) =>
        log.info(s"Server could not be bound to $host:$port: ${e.getMessage}")
    }

  }
}
