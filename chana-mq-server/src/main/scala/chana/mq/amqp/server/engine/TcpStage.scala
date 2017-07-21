package chana.mq.amqp.server.engine

import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.util.ByteString
import chana.mq.amqp.model.Frame

final class TcpStage extends GraphStage[FlowShape[ByteString, ByteString]] {

  val in = Inlet[ByteString]("TcpStage.in")
  val out = Outlet[ByteString]("TcpStage.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        if (isClosed(in)) pushDisconnect()
        else pull(in)
      }
    })

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        push(out, grab(in))
      }

      override def onUpstreamFinish(): Unit = {
        pushDisconnect()
        completeStage()
      }
    })

    private def pushDisconnect() = {
      push(out, Frame.DISCONNECT_FRAME_BODY)
    }
  }
}