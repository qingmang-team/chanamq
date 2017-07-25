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

final class TcpStage extends GraphStage[FlowShape[ByteString, Either[Control, ByteString]]] {

  val in = Inlet[ByteString]("TcpStage.in")
  val out = Outlet[Either[Control, ByteString]]("TcpStage.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        if (isClosed(in)) {
          emitDisconnect()
        } else {
          pull(in)
        }
      }
    })

    setHandler(in, new InHandler {
      override def onPush() {
        push(out, Right(grab(in)))
      }

      override def onUpstreamFinish() {
        emitDisconnect()
        completeStage()
      }
    })

    private def emitDisconnect() {
      emit(out, Left(Disconnect))
    }
  }
}