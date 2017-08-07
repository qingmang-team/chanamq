package chana.mq.amqp.server.engine

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.stream.FlowShape
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.MergePreferred
import akka.stream.scaladsl.Source
import akka.util.ByteString
import chana.mq.amqp.ServerSettings
import chana.mq.amqp.server.engine
import scala.concurrent.duration._

object ServerBluePrint {

  /**
   *
   *                        +------------+  +------------+
   * incoming -> discStage ->            |  |            -> outgoing
   *                        |  merge     -> | frameStage |
   *             control   ->            |  |            |
   *                        +------------+  +------------+
   *
   */
  def apply(settings: ServerSettings, log: LoggingAdapter)(implicit system: ActorSystem) = {
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val control = builder.add(Source.tick(0.seconds, 1.micros, Left(engine.Tick)).buffer(1, OverflowStrategy.dropNew))
      val incoming = builder.add(Flow[ByteString])
      val disconnectStage = builder.add(new DisconnectStage())
      val frameStage = builder.add(new FrameStage())

      val merge = builder.add(MergePreferred[Either[engine.Control, ByteString]](1))
      incoming ~> disconnectStage ~> merge.preferred
      control ~> merge.in(0)
      merge ~> frameStage.in

      FlowShape(incoming.in, frameStage.out)
    })
  }

}
