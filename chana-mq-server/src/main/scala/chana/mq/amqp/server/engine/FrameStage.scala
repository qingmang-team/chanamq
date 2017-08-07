package chana.mq.amqp.server.engine

import akka.actor.ActorSystem
import akka.cluster.sharding.ClusterSharding
import akka.event.Logging
import akka.pattern.ask
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.stream.stage.TimerGraphStageLogic
import akka.util.ByteString
import akka.util.Timeout
import chana.mq.amqp
import chana.mq.amqp.Message
import chana.mq.amqp.engine.CommandAssembler
import chana.mq.amqp.engine.FrameParser
import chana.mq.amqp.entity.ExchangeEntity
import chana.mq.amqp.entity.MessageEntity
import chana.mq.amqp.entity.QueueEntity
import chana.mq.amqp.method.AMQClass
import chana.mq.amqp.method.Access
import chana.mq.amqp.method.Basic
import chana.mq.amqp.method.Channel
import chana.mq.amqp.method.Confirm
import chana.mq.amqp.method.Connection
import chana.mq.amqp.method.Exchange
import chana.mq.amqp.method.Queue
import chana.mq.amqp.method.Tx
import chana.mq.amqp.model.AMQChannel
import chana.mq.amqp.model.AMQCommand
import chana.mq.amqp.model.AMQConnection
import chana.mq.amqp.model.AMQConsumer
import chana.mq.amqp.model.AMQP
import chana.mq.amqp.model.ErrorCodes
import chana.mq.amqp.model.Frame
import chana.mq.amqp.model.LongString
import chana.mq.amqp.model.ProtocolVersion
import chana.mq.amqp.model.VirtualHost
import chana.mq.amqp.server.service.ServiceBoard
import java.util.UUID
import scala.collection.immutable
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

final class FrameStage()(implicit system: ActorSystem) extends GraphStage[FlowShape[Either[Control, ByteString], ByteString]] {
  type AMQMethod = AMQClass#Method

  import system.dispatcher
  private implicit val timeout: Timeout = system.settings.config.getInt("chana.mq.internal.timeout").seconds

  private val log = Logging(system, this.getClass)

  private lazy val connection = {
    val conn = new AMQConnection()
    val connConfig = system.settings.config.getConfig("chana.mq.amqp.connection")

    conn.channelMax = connConfig.getInt("channel-max")
    conn.frameMax = connConfig.getInt("frame-max")
    conn.frameMin = connConfig.getInt("frame-min")

    val heartbeatDelay = connConfig.getInt("heartbeat")
    conn.writerHeartbeat = heartbeatDelay.seconds
    conn.readerHeartbeat = (heartbeatDelay * 2).seconds

    conn
  }

  var vhost: String = _

  val in = Inlet[Either[Control, ByteString]]("tcp-in")
  val out = Outlet[ByteString]("tcp-out")

  override val shape = new FlowShape[Either[Control, ByteString], ByteString](in, out)

  private val id = System.identityHashCode(FrameStage.this)

  private def outActiveChannels = connection.channels.values.filter(_.hasConsumers).filter(_.isOutFlowActive)

  private def exchangeSharding = ClusterSharding(system).shardRegion(ExchangeEntity.typeName)
  private def messageSharding = ClusterSharding(system).shardRegion(MessageEntity.typeName)
  private def queueSharding = ClusterSharding(system).shardRegion(QueueEntity.typeName)

  private val serviceBoard = ServiceBoard(system)
  def idService = serviceBoard.idService

  override def createLogic(inheritedAttributes: Attributes) = new TimerGraphStageLogic(shape) {
    // TODO -- use it or not?
    private var isClientExpectingResponse = false

    /**
     * Heartbeat frames are sent about every timeout / 2 seconds
     */
    private var isHeartbeatTime = false

    override protected def onTimer(timerKey: Any) {
      log.debug(s"$id onTimer")
      isHeartbeatTime = true
    }

    setHandler(in, new HandshakeInHandler())
    setHandler(out, new HandshakeOutHandler())

    private def pushHeartbeat() = push(out, Frame.HEARTBEAT_FRAME_BODY)

    private def pushConnectionClose(channelId: Int, errorCode: Int, message: String, causeMethodClassId: Int, causeMethodId: Int) {
      //markChannelAwaitingCloseOk(channelId)
      val future = completeAndCloseAllChannels()

      val callback = getAsyncCallback[Iterable[Boolean]] { _ =>
        val method = Connection.Close(errorCode, message, causeMethodClassId, causeMethodId)
        val command = AMQCommand(connection.channel0, method)

        push(out, command.render)

        // do not call completeStage() here, the client may send back CloseOk or sth? 
        // and cause error: Bad frame end marker: 0
      }

      future.foreach(callback.invoke)
    }

    // TODO when to send Connection.Blocked and Channel.Flow
    private def pushConnectionBlocked(channel: AMQChannel, reason: String) {
      val command = AMQCommand(channel, Connection.Blocked(reason))

      push(out, command.render)
    }

    private def pushConnectionUnblocked(channel: AMQChannel) {
      val command = AMQCommand(channel, Connection.Unblocked)

      push(out, command.render)
    }

    private def completeAndCloseAllChannels(): Future[Iterable[Boolean]] = {
      Future.sequence(connection.channels.values map completeAndCloseChannel) flatMap { _ =>
        Future.sequence(connection.exclusiveQueues map { queue =>
          (queueSharding ? QueueEntity.ForceDelete(vhost, queue, connection.id)).mapTo[Boolean]
        })
      }
    }

    private def completeAndCloseChannel(channel: AMQChannel): Future[Iterable[Boolean]] = {
      Future.sequence(channel.allConsumers map { consumer => (queueSharding ? QueueEntity.ConsumerCancelled(vhost, consumer.queue, consumer.tag, connection.id, consumer.channel.id)).mapTo[Boolean] }) map { done =>
        channel.clearConsumers
        connection.channels -= channel.id
        done
      }
    }

    private class HandshakeOutHandler extends OutHandler {

      override def onPull() {
        log.debug(s"$id outport: onPull ==>")
        pull(in)
      }
    }

    private class HandshakeInHandler extends InHandler {
      var stash = ByteString.empty

      override def onUpstreamFinish() {
        completeStage()
      }

      override def onPush() {
        log.debug(s"$id ==> inport: onPush")

        grab(in) match {
          case Left(Disconnect) =>
            completeStage()

          case Left(Tick) =>
            pull(in)

          case Right(bytes) =>
            stash ++= bytes
            log.debug(s"stash: $stash")
            if (stash.length >= AMQP.LENGTH_OF_PROTOCAL_HEADER) {
              val (header, rest) = stash.splitAt(AMQP.LENGTH_OF_PROTOCAL_HEADER)
              header.toArray match {
                case Array('A', 'M', 'Q', 'P', 0, 0, 9, 1) =>
                  log.info("recv: AMQ protocol header")

                  val serverProperties = Map(
                    "product" -> "chana.mq",
                    "version" -> "0.1.0",
                    "chana.mq.build" -> "1"
                  )

                  val mechanisms = LongString("PLAIN".getBytes)
                  val locales = LongString("en_US".getBytes)

                  val method = Connection.Start(
                    ProtocolVersion.V_0_91.majorVersion,
                    ProtocolVersion.V_0_91.actualMinorVersion,
                    serverProperties,
                    mechanisms,
                    locales
                  )

                  val command = AMQCommand(connection.channel0, method)

                  push(out, command.render)

                  setHandler(in, new FrameInHandler)
                  setHandler(out, new FrameOutHandler)

                case _ =>
                  log.warning(s"recv: $stash")
                  push(out, ByteString(ProtocolVersion.V_0_91.protocolHeader))
              }
              stash = rest
            } else {
              pull(in)
            }
        }
      }
    }

    private class FrameOutHandler extends OutHandler {

      /**
       * Is called when the output port is ready to emit the next element,
       * push(out, elem) is now allowed to be called on this port.
       */
      override def onPull() {
        log.debug(s"$id outport: onPull ==>")
        pull(in)
      }
    }

    private class FrameInHandler extends InHandler {
      val frameParser = new FrameParser()
      var commandAssembler = new CommandAssembler(connection)

      var pendingPayload = ByteString.newBuilder

      override def onUpstreamFinish() {
        log.info("upstream finished, going to close all channels")
        completeAndCloseAllChannels()
        completeStage()
      }

      /**
       * is called when the input port has now a new element. Now it is possible
       * to acquire this element using grab(in) and/or call pull(in) on the port
       * to request the next element. It is not mandatory to grab the element,
       * but if it is pulled while the element has not been grabbed it will drop
       * the buffered element.
       *
       * NOTE: it seems once a pull(in) causes state changing to onPush, we'll
       * be stacked on onPush until client pushing sth. i.e. no chance to switch
       * back to out.onPull anymore
       */
      override def onPush() {
        log.debug(s"$id ==> inport: onPush")

        grab(in) match {
          case Left(Disconnect) =>
            log.info(s"$id Tcp upstream disconnected, going to close all channels")
            completeAndCloseAllChannels()
            completeStage()

          case Left(Tick) =>
            pushHeatbeatOrPendingOrMessagesOrPull()

          case Right(bytes) =>
            val frames = frameParser.onReceive(bytes.iterator).iterator

            var publishCommands = Vector[AMQCommand[Basic.Publish]]()
            var ackCommands = Vector[AMQCommand[Basic.Ack]]()
            var otherCommands = Vector[AMQCommand[AMQMethod]]()
            var lastCommandOnThisPush: Option[AMQCommand[AMQMethod]] = None

            var shouldClose = false
            while (frames.hasNext && !shouldClose) {
              frames.next() match {
                case FrameParser.Ok(Frame(Frame.HEARTBEAT, _, _)) =>
                  // TODO check/count heartbeat of this connection? or just let client to deal with it?
                  log.info(s"$id received heartbeat")

                case FrameParser.Ok(frame) =>
                  log.debug(s"$id got frame: $frame")

                  commandAssembler.onReceive(frame) match {
                    case CommandAssembler.Ok(command) =>
                      command.method match {
                        case _: Basic.Ack     => ackCommands :+= command.asInstanceOf[AMQCommand[Basic.Ack]]
                        case _: Basic.Publish => publishCommands :+= command.asInstanceOf[AMQCommand[Basic.Publish]]
                        case _                => otherCommands :+= command
                      }

                      lastCommandOnThisPush = Some(command)

                      // reset for next command
                      commandAssembler = new CommandAssembler(connection)
                    case error @ CommandAssembler.Error(frame, reason) =>
                      pushConnectionClose(frame.channel, ErrorCodes.FRAME_ERROR, reason, 0, 0)
                      log.error(s"$id $error")
                      shouldClose = true

                    case _ => // Go on - wait for next frame
                  }

                case error @ FrameParser.Error(errorCode, message) =>
                  pushConnectionClose(0, errorCode, message, 0, 0)
                  log.error(s"$id $error")
                  shouldClose = true
              }
            } // end frames loop

            if (!shouldClose) {
              lastCommandOnThisPush match {
                case None =>
                  // there is no incoming command, do't forget at least to pull(in)
                  pushHeatbeatOrPendingOrMessagesOrPull

                case Some(last) =>
                  if (otherCommands.length > 1) {
                    log.warning(s"$id Got batch other commands: $otherCommands")
                  }
                  receivedCommands(otherCommands, last)

                  if (publishCommands.nonEmpty) {
                    log.debug(s"$id got publish commands: ${publishCommands.size}")

                    // we'll drop publishes of which channel is not in-flow active 
                    val (actives, inactives) = publishCommands.partition(_.channel.isInFlowActive)
                    receivedPublishes(actives, isLastCommand = last.method.isInstanceOf[Basic.Publish])

                    log.warning(s"channels ${inactives.map(_.channel.id).toSet} should have been Channel.Flow(false), but are still sending content")
                  }

                  if (ackCommands.nonEmpty) {
                    log.debug(s"$id got ack commands: ${ackCommands.size}")
                    receivedAcks(ackCommands, isLastCommand = last.method.isInstanceOf[Basic.Ack])
                  }
              }
            }
        }
      }

      private def pushHeatbeatOrPendingOrMessagesOrPull() = {
        if (isHeartbeatTime) {

          pushHeartbeat()
          log.info(s"$id pushed heartbeat")
          isHeartbeatTime = false

        } else if (pendingPayload.nonEmpty) {

          val payload = pendingPayload.result
          pendingPayload.clear
          push(out, payload)

        } else {
          val activeChannels = outActiveChannels
          if (activeChannels.nonEmpty) {

            // fair play on consumers for each channel
            val future = Future.sequence(activeChannels map { channel =>
              val consumer = channel.nextRoundConsumer()

              val prefetchCount = if (consumer.autoAck) {
                channel.prefetchCount
              } else {
                val nUnacked = if (channel.prefetchGlobal) channel.nUnacks else consumer.nUnacks
                channel.prefetchCount - nUnacked
              }

              log.debug(s"$id prefetchCount: $prefetchCount consumer unacks: ${consumer.nUnacks}")

              if (prefetchCount > 0) {
                (queueSharding ? QueueEntity.Pull(vhost, consumer.queue, Some(consumer.tag), connection.id, consumer.channel.id, prefetchCount, channel.prefetchSize, consumer.autoAck)).mapTo[Vector[Long]] flatMap { msgIds =>
                  log.debug(s"$id pulled: $msgIds")
                  Future.sequence(msgIds map { msgId =>
                    (messageSharding ? MessageEntity.Get(msgId.toString)).mapTo[Option[Message]]
                  }) map { msgs => (consumer, msgIds, msgs) }
                }
              } else {
                Future { (consumer, Vector.empty, Vector.empty) }
              }
            }) andThen {
              case Success(_) =>
              case Failure(e) => log.error(e, e.getMessage)
            }

            val callback = getAsyncCallback[Iterable[(AMQConsumer, Vector[Long], Vector[Option[Message]])]] { consumerAndMsgs =>
              val body = consumerAndMsgs.foldLeft(ByteString.newBuilder) {
                case (acc, (consumer @ AMQConsumer(channel, consumerTag, queue, autoAck), msgIds, msgs)) =>
                  val nMsgs = msgIds.size
                  if (nMsgs > 0) log.info(s"$id delivered msgs: $nMsgs")

                  val deliveryMsgs = channel.goingToDeliveryMsgs(msgIds, consumer, autoAck)

                  if (autoAck) {
                    // Unrefer should happen after message got. Unrefer could be async
                    msgIds foreach { msgId => messageSharding ! MessageEntity.Unrefer(msgId.toString) }
                  }

                  (deliveryMsgs zip msgs).foldLeft(acc) {
                    case (acc, (deliveryMsg, Some(Message(msgId, header, body, exchange, routingKey, ttl)))) =>
                      val method = Basic.Deliver(consumerTag, deliveryMsg.deliveryTag, redelivered = deliveryMsg.nDelivery > 1, exchange, routingKey)
                      val command = AMQCommand(channel, method, header, body)

                      acc ++= command.render
                    case (acc, (_, None)) =>
                      acc
                  }

                  acc
              }

              val payload = body.result
              if (payload.nonEmpty) {
                push(out, payload)
              } else {
                log.debug(s"$id delivered zero msgs")
                pull(in)
              }
            }

            future.foreach(callback.invoke)
          } else {

            pull(in)

          }
        }
      }

      /**
       * Section 4.7 of the AMQP 0-9-1 core specification explains the
       * conditions under which ordering is guaranteed: messages published
       * in one channel, passing through one exchange and one queue and
       * one outgoing channel will be received in the same order that
       * they were sent.
       */
      private def receivedPublishes(publishes: Vector[AMQCommand[Basic.Publish]], isLastCommand: Boolean) {
        var msgIdToCommand = Map[Long, AMQCommand[Basic.Publish]]()
        var msgIdToConfirmNo = Map[Long, Long]()
        var channelToConfirmedNo = mutable.Map[AMQChannel, mutable.LinkedHashSet[Long]]()
        val future = Future.sequence(publishes.groupBy(x => x.method.exchange) map {
          case (exchange, commands) =>
            val msgIds = idService.nextIds(commands.size)

            Future.sequence((msgIds zip commands) map {
              case (msgId, command @ AMQCommand(channel, publish, header, body)) =>
                log.debug(s"Got $msgId, ${try { new String(body.getOrElse(Array())) } catch { case ex: Throwable => "" }}")

                channel.isDoingPublishing = true
                channel.mode match {
                  case AMQChannel.Confirm =>
                    val confirmNo = channel.nextConfirmNumber()
                    msgIdToConfirmNo += (msgId -> confirmNo)
                    channelToConfirmedNo.getOrElseUpdate(channel, new mutable.LinkedHashSet[Long]()) += confirmNo
                  case _ =>
                }

                val isPersist = header match {
                  case None => false
                  case Some(props) => props.deliveryMode match {
                    case null => false
                    case x    => x == 2
                  }
                }

                val ttl = header match {
                  case None => None
                  case Some(props) => props.expiration match {
                    case null => None
                    case x =>
                      try {
                        Some(x.toLong)
                      } catch {
                        case e: Throwable =>
                          log.error(e, e.getMessage)
                          None
                      }
                  }
                }

                (messageSharding ? MessageEntity.Received(msgId.toString, header, body, publish.exchange, publish.routingKey, ttl)).mapTo[Boolean] map { _ =>
                  msgIdToCommand += (msgId -> command)
                  ExchangeEntity.Publish(msgId, publish.routingKey, publish.mandatory, publish.immediate, body.map(_.length).getOrElse(0), isPersist, ttl)
                }
            }) flatMap {
              case pubs =>
                (exchangeSharding ? ExchangeEntity.Publishs(vhost, exchange, pubs, connection.id)).mapTo[Vector[(Long, Boolean, Boolean)]]
            }
        }) andThen {
          case Success(_) => log.info(s"$id published msgs: ${publishes.size}")
          case Failure(e) => log.error(e, e.getMessage)
        }

        val callback = getAsyncCallback[immutable.Iterable[Vector[(Long, Boolean, Boolean)]]] { publishResults =>

          val (mandatoryReturns, immediateReturns) = publishResults.flatten.foldLeft((Vector[AMQCommand[Basic.Publish]](), Vector[AMQCommand[Basic.Publish]]())) {
            case ((mandatoryReturns, immediateReturns), (msgId, isNonRouted, isNonDeliverable)) =>
              msgIdToCommand.get(msgId) match {
                case Some(command) =>
                  val channel = command.channel

                  if (command.method.immediate && isNonDeliverable) {
                    channel.mode match {
                      case AMQChannel.Confirm =>
                        msgIdToConfirmNo.get(msgId) foreach { confirmNo =>
                          channelToConfirmedNo.get(channel) foreach { confirmNos => confirmNos -= confirmNo }
                        }
                      case _ =>
                    }

                    (mandatoryReturns, immediateReturns :+ command)
                  } else {
                    if (command.method.mandatory && isNonRouted) {
                      channel.mode match {
                        case AMQChannel.Confirm =>
                          msgIdToConfirmNo.get(msgId) foreach { confirmNo =>
                            channelToConfirmedNo.get(channel) foreach { confirmNos => confirmNos -= confirmNo }
                          }
                        case _ =>
                      }

                      (mandatoryReturns :+ command, immediateReturns)
                    } else {

                      (mandatoryReturns, immediateReturns)
                    }
                  }

                case None => // this should not happen
                  (mandatoryReturns, immediateReturns)
              }
          }

          pendingPayload = mandatoryReturns.foldLeft(ByteString.newBuilder) {
            case (acc, pubCommand @ AMQCommand(channel, Basic.Publish(_, exchange, routingKey, _, _), header, body)) =>
              val command = AMQCommand(channel, Basic.Return(ErrorCodes.NO_ROUTE, ErrorCodes.noRoute, exchange, routingKey), header, body)
              acc ++= command.render
          }

          pendingPayload = immediateReturns.foldLeft(pendingPayload) {
            case (acc, AMQCommand(channel, Basic.Publish(_, exchange, routingKey, _, _), header, body)) =>
              val command = AMQCommand(channel, Basic.Return(ErrorCodes.NO_CONSUMERS, ErrorCodes.noConsumers, exchange, routingKey), header, body)
              acc ++= command.render
          }

          channelToConfirmedNo foreach {
            case (channel, confirmNos) =>
              var confirmed = Vector[(Long, Boolean)]()
              val ns = confirmNos.iterator
              var n = 0L

              while (ns.hasNext) {
                val nx = ns.next()
                if (n == 0) {
                  confirmed :+= (nx, false)
                } else if (nx - n == 1) {
                  confirmed = confirmed.dropRight(1)
                  confirmed :+= (nx, true)
                } else {
                  confirmed :+= (nx, false)
                }

                n = nx
              }

              pendingPayload = confirmed.foldLeft(pendingPayload) {
                case (acc, (n, multiple)) =>
                  val command = AMQCommand(channel, Basic.Ack(n, multiple))
                  acc ++= command.render
              }
          }

          // if not isLastCommand, means we are still under same onPush (
          // received multiple commands on one push), we should not push 
          // or pull on port until isLastCommand.
          if (isLastCommand) {
            pushHeatbeatOrPendingOrMessagesOrPull()
          }
        }

        future.foreach(callback.invoke)
      }

      private def receivedAcks(acks: Vector[AMQCommand[Basic.Ack]], isLastCommand: Boolean) {
        log.debug(s"$id recv $acks")
        val start = System.currentTimeMillis

        val queueToAckMsgIds = mutable.Map[String, mutable.Set[Long]]()
        acks foreach {
          case AMQCommand(channel, Basic.Ack(deliveryTag, multiple), _, _) =>
            if (multiple) {
              val ackTags = channel.getMultipleTagsTill(deliveryTag)

              channel.ackDeliveryTags(ackTags) foreach {
                case (queue, msgId) => queueToAckMsgIds.getOrElseUpdate(queue, new mutable.HashSet[Long]()) += msgId
              }
            } else {
              channel.ackDeliveryTag(deliveryTag) foreach {
                case (queue, msgId) => queueToAckMsgIds.getOrElseUpdate(queue, new mutable.HashSet[Long]()) += msgId
              }
            }
        }

        queueToAckMsgIds map {
          case (queue, msgIds) =>
            msgIds foreach { msgId => messageSharding ! MessageEntity.Unrefer(msgId.toString) }
            queueSharding ! QueueEntity.Acked(vhost, queue, msgIds)
        }

        log.info(s"$id acked in ${System.currentTimeMillis - start}ms")

        if (isLastCommand) {
          pushHeatbeatOrPendingOrMessagesOrPull()
        }
      }

      /**
       * Reject one or more incoming messages.
       *
       * This method allows a client to reject one or more incoming messages. It
       * can be used to interrupt and cancel large incoming messages, or return
       * untreatable messages to their original queue. This method is also used
       * by the server to inform publishers on channels in confirm mode of unhandled
       * messages. If a publisher receives this method, it probably needs to
       * republish the offending messages.
       *
       * The server SHOULD be capable of accepting and processing the Nack method
       * while sending message content with a Deliver or Get-Ok method. I.e. the
       * server should read and process incoming methods while sending output frames.
       * To cancel a partially-send content, the server sends a content body frame
       * of size 1 (i.e. with no data except the frame-end octet).
       *
       * The server SHOULD interpret this method as meaning that the client is
       * unable to process the message at this time.
       *
       * The client MUST NOT use this method as a means of selecting messages to process.
       *
       * A client publishing messages to a channel in confirm mode SHOULD be capable
       * of accepting and somehow handling the Nack method.
       */
      private def receivedNack(channel: AMQChannel, deliveryTag: Long, multiple: Boolean, requeue: Boolean, isLastCommand: Boolean) {
        val queueToUnackMsgIds = mutable.Map[String, mutable.Set[Long]]()
        if (multiple) {
          val uackTags = channel.getMultipleTagsTill(deliveryTag)

          channel.ackDeliveryTags(uackTags) foreach {
            case (queue, msgId) => queueToUnackMsgIds.getOrElseUpdate(queue, new mutable.HashSet[Long]()) += msgId
          }
        } else {
          channel.ackDeliveryTag(deliveryTag) foreach {
            case (queue, msgId) => queueToUnackMsgIds.getOrElseUpdate(queue, new mutable.HashSet[Long]()) += msgId
          }
        }

        if (requeue) {
          queueToUnackMsgIds map {
            case (queue, msgIds) =>
              queueSharding ! QueueEntity.Requeue(vhost, queue, msgIds)
          }
        } else {
          queueToUnackMsgIds map {
            case (queue, msgIds) =>
              msgIds foreach { msgId => messageSharding ! MessageEntity.Unrefer(msgId.toString) }
              queueSharding ! QueueEntity.Acked(vhost, queue, msgIds)
          }
        }

        if (isLastCommand) {
          pushHeatbeatOrPendingOrMessagesOrPull()
        }
      }

      /**
       * Redeliver unacknowledged messages.
       * This method asks the server to redeliver all unacknowledged messages on
       * a specified channel. Zero or more messages may be redelivered. This method
       * replaces the asynchronous Recover.
       *
       * The server MUST set the redelivered flag on all messages that are resent.
       *
       * @params requeue If this field is zero, the message will be redelivered
       *         to the original recipient. If this bit is 1, the server will
       *         attempt to requeue the message, potentially then delivering it
       *         to an alternative subscriber.
       */
      private def receivedRecover(channel: AMQChannel, requeue: Boolean, isLastCommand: Boolean) {
        val consumerToUnackMsgIds = channel.unackedDeliveryTagToMsg.foldLeft(mutable.Map[AMQConsumer, mutable.Set[Long]]()) {
          case (acc, (deliveryTag, msg)) =>
            acc.getOrElseUpdate(msg.consumer, new mutable.HashSet[Long]()) += msg.msgId
            acc
        }

        if (requeue) {
          consumerToUnackMsgIds map {
            case (consumer, msgIds) => queueSharding ! QueueEntity.Requeue(vhost, consumer.queue, msgIds)
          }

          channel.unackedDeliveryTagToMsg.clear
          channel.unackedMsgIdToDeliveryMsg.clear

          if (isLastCommand) {
            pushHeatbeatOrPendingOrMessagesOrPull()
          }

        } else {

          val future = Future.sequence(consumerToUnackMsgIds map {
            case (consumer, msgIds) =>
              log.debug(s"$id pulled: $msgIds")
              val sortedMsgIds = msgIds.toVector.sorted
              Future.sequence(sortedMsgIds map { msgId =>
                (messageSharding ? MessageEntity.Get(msgId.toString)).mapTo[Option[Message]]
              }) map { msgs => (consumer, sortedMsgIds, msgs) }
          }) andThen {
            case Success(_) =>
            case Failure(e) => log.error(e, e.getMessage)
          }

          val callback = getAsyncCallback[mutable.Iterable[(AMQConsumer, Vector[Long], Vector[Option[Message]])]] { consumerAndMsgs =>
            // Clear all unacked delivery tag. We'll deal with unackedMsgToDeliveryMsg later via removeUnReferredMsgId()
            channel.unackedDeliveryTagToMsg.clear

            pendingPayload = consumerAndMsgs.foldLeft(pendingPayload) {
              case (acc, (consumer @ AMQConsumer(channel, consumerTag, queue, autoAck), msgIds, msgs)) =>
                val nMsgs = msgIds.size
                if (nMsgs > 0) log.info(s"$id delivered msgs: $nMsgs")

                val deliveryMsgs = channel.goingToDeliveryMsgs(msgIds, consumer, autoAck)
                channel.removeUnReferredMsgId()

                (deliveryMsgs zip msgs).foldLeft(acc) {
                  case (acc, (deliveryMsg, Some(Message(msgId, header, body, exchange, routingKey, ttl)))) =>
                    val method = Basic.Deliver(consumerTag, deliveryMsg.deliveryTag, redelivered = true, exchange, routingKey)
                    val command = AMQCommand(channel, method, header, body)

                    acc ++= command.render
                  case (acc, (_, None)) =>
                    acc
                }

                acc
            }

            if (isLastCommand) {
              pushHeatbeatOrPendingOrMessagesOrPull()
            }
          }

          future.foreach(callback.invoke)
        }
      }

      private def receivedCommands(commands: Vector[AMQCommand[AMQMethod]], lastCommand: AMQCommand[AMQMethod]) {
        log.debug(s"recv commands $commands")
        commands foreach (command => receivedCommand(command, command eq lastCommand))
      }

      private def receivedCommand(command: AMQCommand[AMQMethod], isLastCommand: Boolean) {
        val method = command.method
        val channel = command.channel
        log.info(s"$id channel:${command.channel.id} ${method.className}.${method}")
        isClientExpectingResponse = method.expectResponse

        method.classId match {
          case Basic.`id`      => receivedBasicMethod(channel, method, isLastCommand)
          case Queue.`id`      => receivedQueueMethod(channel, method, isLastCommand)
          case Channel.`id`    => receivedChannelMethod(channel, method, isLastCommand)
          case Exchange.`id`   => receivedExchangeMethod(channel, method, isLastCommand)
          case Connection.`id` => receivedConnectionMethod(channel, method, isLastCommand)
          case Access.`id`     => receivedAccessMethod(channel, method, isLastCommand)
          case Tx.`id`         => receivedTxMethod(channel, method, isLastCommand)
          case Confirm.`id`    => receivedConfirmMethod(channel, method, isLastCommand)
        }

        // reset for next command
        isClientExpectingResponse = false
      }

      private def receivedConnectionMethod(channel: AMQChannel, method: AMQMethod, isLastCommand: Boolean) {
        method match {
          case Connection.StartOk(clientProperties, mechanism, response, locale) =>
            if (mechanism == null || mechanism.length == 0) {
              pushConnectionClose(channel.id, ErrorCodes.CONNECTION_FORCED, "No Sasl mechanism was specified", method.classId, method.id)
            } else {
              SaslMechanism.getSaslMechanism(mechanism.split(" ")) match {
                case Some(saslMechanism) =>
                  val x = saslMechanism.handleResponse(response.bytes)
                  log.info(x.toString)

                  val command = AMQCommand(connection.channel0, Connection.Tune(connection.channelMax, connection.frameMax, connection.writerHeartbeat.toSeconds.toInt))

                  push(out, command.render)
                case None =>
                  pushConnectionClose(channel.id, ErrorCodes.ACCESS_REFUSED, "Sasl mechanism aceess refused", method.classId, method.id)
              }
            }

          case Connection.TuneOk(channelMax, frameMax, heartbeat) =>
            if (heartbeat > 0) {
              connection.writerHeartbeat = heartbeat.second
              connection.readerHeartbeat = (heartbeat * 2).second
            }

            val brokerFrameMax = connection.frameMax //  - Frame.NON_BODY_SIZE
            if (frameMax > brokerFrameMax) {
              pushConnectionClose(0, ErrorCodes.SYNTAX_ERROR, s"Attempt to set max frame size to $frameMax greater than the broker will allow: $brokerFrameMax", method.classId, method.id)
            } else if (frameMax > 0 && frameMax < connection.frameMin) {
              pushConnectionClose(0, ErrorCodes.SYNTAX_ERROR, s"Attempt to set max frame size to $frameMax which is smaller than the specification defined minimum: ${connection.frameMin}", method.classId, method.id)
            } else {
              connection.frameMax = if (frameMax == 0) brokerFrameMax else frameMax
              connection.channelMax = if (channelMax == 0 || channelMax > 0xFFFF) 0xFFFF else channelMax
            }

            /**
             * The client should start sending heartbeats after receiving a Connection.Tune method, and start
             * monitoring heartbeats after receiving Connection.Open. The server should start sending and
             * monitoring heartbeats after receiving Connection.Tune-Ok.
             */
            if (isLastCommand) { // Connection.TuneOk could be batch followed by Connection.Open
              pushHeartbeat()
              log.info(s"$id pushed heartbeat")
            } else {
              isHeartbeatTime = true
            }
            schedulePeriodically(None, connection.writerHeartbeat)

          case Connection.Open(virtualHost, capabilities, insist) =>
            val vhId = if (virtualHost != null && virtualHost.charAt(0) == '/') {
              virtualHost.substring(1)
            } else {
              virtualHost
            }

            connection.virtualHost = VirtualHost.getVirtualHost(vhId) match {
              case Some(x) => x
              case None    => VirtualHost.createVirtualHost(vhId) // TODO
            }

            vhost = vhId

            if (connection.virtualHost == null) {
              pushConnectionClose(channel.id, ErrorCodes.NOT_FOUND, "Unknown virtual host: '" + virtualHost + "'", method.classId, method.id)
            } else {
              // Check virtualhost access
              if (!connection.virtualHost.isActive) {
                //String redirectHost = addressSpace.getRedirectHost(getPort());
                //if(redirectHost != null) {
                // sendConnectionClose(0, new AMQFrame(0, new ConnectionRedirectBody(getProtocolVersion(), AMQShortString.valueOf(redirectHost), null)));
                //} else {
                //sendConnectionClose(ErrorCodes.CONNECTION_FORCED, "Virtual host '" + addressSpace.getName() + "' is not active", 0);
                //}
              } else {
                try {
                  if (connection.virtualHost.authoriseCreateConnection(connection)) {
                    val command = AMQCommand(connection.channel0, Connection.OpenOk(virtualHost))

                    push(out, command.render)
                    //_state = ConnectionState.OPEN
                  } else {
                    pushConnectionClose(channel.id, ErrorCodes.ACCESS_REFUSED, "Connection refused", method.classId, method.id)
                  }
                } catch {
                  case e: Throwable =>
                    log.error(e, e.getMessage)
                    pushConnectionClose(channel.id, ErrorCodes.ACCESS_REFUSED, e.getMessage, method.classId, method.id)
                }
              }
            }

          case Connection.Close(replyCode, replyText, classId, methodId) =>
            val future = completeAndCloseAllChannels() andThen {
              case Success(_) =>
              case Failure(e) => log.error(e, e.getMessage)
            }

            val callback = getAsyncCallback[Iterable[Boolean]] { _ =>
              val method = Connection.CloseOk
              val command = AMQCommand(connection.channel0, method)

              push(out, command.render)
              completeStage()
            }

            future.foreach(callback.invoke)

          case Connection.SecureOk(response) =>
            log.info(s"TODO Method not be processed: ${method.getClass.getName}")

          case Connection.CloseOk =>
            log.info(s"TODO Method not be processed: ${method.getClass.getName}")
        }
      }

      private def receivedChannelMethod(channel: AMQChannel, method: AMQMethod, isLastCommand: Boolean) {
        method match {
          case Channel.Open(reserved_1) =>
            //assertState(ConnectionState.OPEN)

            // Protect the broker against out of order frame request.
            val response = if (connection.virtualHost == null) {
              pushConnectionClose(channel.id, ErrorCodes.COMMAND_INVALID, "Virtualhost has not yet been set. Connection.Open has not been called.", method.classId, method.id)
            } else if (connection.channels.get(channel.id).isDefined /*|| channelAwaitingClosure(channelId)*/ ) {
              pushConnectionClose(channel.id, ErrorCodes.CHANNEL_ERROR, "Channel " + channel.id + " already exists", method.classId, method.id)
            } else if (channel.id > connection.channelMax) {
              pushConnectionClose(channel.id, ErrorCodes.CHANNEL_ERROR, "Channel " + channel.id + " cannot be created as the max allowed channel id is " + connection.channelMax, method.classId, method.id)
            } else {
              connection.channels += (channel.id -> channel)

              val command = AMQCommand(channel, Channel.OpenOk(LongString(channel.id.toString.getBytes)))

              push(out, command.render)
            }

          case Channel.Close(replyCode, replyText, classId, methodId) =>
            val future = completeAndCloseChannel(channel) andThen {
              case Success(_) =>
              case Failure(e) => log.error(e, e.getMessage)
            }

            val callback = getAsyncCallback[Iterable[Boolean]] { _ =>
              val command = AMQCommand(channel, Channel.CloseOk)

              push(out, command.render)
            }

            future.foreach(callback.invoke)

          case Channel.CloseOk =>
            log.info(s"TODO Method not be processed: ${method.getClass.getName}")

          case Channel.Flow(active) =>
            channel.isOutFlowActive = active
            val command = AMQCommand(channel, Channel.FlowOk(active))

            push(out, command.render)

          case Channel.FlowOk(active) =>
            // TODO when to inactive channel's in-flow by sending Channel.Flow(active) to a specific channel
            channel.isInFlowActive = active
        }
      }

      private def receivedExchangeMethod(channel: AMQChannel, method: AMQMethod, isLastCommand: Boolean) {
        method match {
          case Exchange.Declare(reserved_1, exchange, tpe, passive, durable, autoDelete, internal, nowait, arguments) =>
            val future = (exchangeSharding ? ExchangeEntity.Existed(vhost, exchange)).mapTo[Boolean] flatMap {
              case true =>
                log.debug(s"Exchange $exchange existed")
                Future.successful((true, false))
              case false =>
                if (passive) {
                  log.warning(s"Exchange $exchange does not exist when passive declared")
                  Future.successful((false, false))
                } else {
                  log.debug(s"Exchange $exchange does not exist, will create it")
                  (exchangeSharding ? ExchangeEntity.Declare(vhost, exchange, tpe, durable, autoDelete, internal, arguments)).mapTo[Boolean] map (created => (false, created))
                }
            } andThen {
              case Success(_) =>
              case Failure(e) => log.error(e, e.getMessage)
            }

            val callback = getAsyncCallback[(Boolean, Boolean)] {
              case (existed, created) =>
                if (passive && !existed) {
                  pushConnectionClose(channel.id, ErrorCodes.NOT_FOUND, s"Exchange $exchange does not exists when passive declared", method.classId, method.id)
                } else {
                  val command = AMQCommand(channel, Exchange.DeclareOk)

                  push(out, command.render)
                }
            }

            future.foreach(callback.invoke)

          case Exchange.Delete(reserved_1, exchange, ifUnused, nowait) =>
            val future = (exchangeSharding ? ExchangeEntity.Existed(vhost, exchange)).mapTo[Boolean] flatMap {
              case true =>
                (exchangeSharding ? ExchangeEntity.Delete(vhost, exchange, ifUnused, connection.id)).mapTo[Boolean] map (deleted => (true, deleted))
              case false =>
                Future.successful((false, false))
            } andThen {
              case Success(_) =>
              case Failure(e) => log.error(e, e.getMessage)
            }

            val callback = getAsyncCallback[(Boolean, Boolean)] {
              // (existed, deleted)
              case (true, deleted) =>
                val command = AMQCommand(channel, Exchange.DeleteOk)

                push(out, command.render)
              case (false, _) =>
                pushConnectionClose(channel.id, ErrorCodes.NOT_FOUND, s"Exchange $exchange does not exists", method.classId, method.id)
            }

            future.foreach(callback.invoke)

          case Exchange.Bind(reserved_1, destination, source, routingKey, nowait, arguments) =>
            log.info(s"TODO Method not be processed: ${method.getClass.getName}")

          case Exchange.Unbind(reserved_1, destination, source, routingKey, nowait, arguments) =>
            log.info(s"TODO Method not be processed: ${method.getClass.getName}")
        }
      }

      private def receivedQueueMethod(channel: AMQChannel, method: AMQMethod, isLastCommand: Boolean) {
        method match {
          case Queue.Declare(reserved_1, queue, passive, durable, exclusive, autoDelete, nowait, arguments) =>
            if (queue != null && queue.startsWith("amp.")) {
              pushConnectionClose(channel.id, ErrorCodes.ACCESS_REFUSED, s"Queue $queue starting with 'amq.' are reserved for internal use by the broker", method.classId, method.id)
            } else {
              val queueName = if (queue == null || queue == "") {
                "tmp." + UUID.randomUUID
              } else {
                queue
              }

              if (exclusive) {
                connection.exclusiveQueues += queue
              }

              val ttl = arguments.get("x-message-ttl") match {
                case Some(x: Int)  => Some(x.longValue)
                case Some(x: Long) => Some(x)
                case _             => None
              }

              val future = (queueSharding ? QueueEntity.Declare(vhost, queueName, durable, exclusive, autoDelete, connection.id, ttl)).mapTo[QueueEntity.Statistics] andThen {
                case Success(_) =>
                case Failure(e) => log.error(e, e.getMessage)
              }

              val callback = getAsyncCallback[QueueEntity.Statistics] { statis =>
                if (passive && !statis.existed) {
                  pushConnectionClose(channel.id, ErrorCodes.NOT_FOUND, s"Queue $queueName does not exists when passive declared", method.classId, method.id)
                } else {
                  val method = Queue.DeclareOk(queueName, statis.queueSize, statis.consumerCount)
                  val command = AMQCommand(channel, method)

                  push(out, command.render)
                }
              }

              future.foreach(callback.invoke)
            }

          case Queue.Bind(reserved_1, queue, exchange, routingKey, nowait, arguments) =>
            if (queue != null && queue.startsWith("amp.")) {
              pushConnectionClose(channel.id, ErrorCodes.ACCESS_REFUSED, s"Queue $queue starting with 'amq.' are reserved for internal use by the broker", method.classId, method.id)
            } else {
              val future = (exchangeSharding ? ExchangeEntity.QueueBind(vhost, exchange, queue, routingKey, arguments)).mapTo[Boolean] andThen {
                case Success(_) =>
                case Failure(e) => log.error(e, e.getMessage)
              }

              val callback = getAsyncCallback[Boolean] { _ =>
                val method = Queue.BindOk
                val command = AMQCommand(channel, method)

                push(out, command.render)
              }

              future.foreach(callback.invoke)
            }

          case Queue.Unbind(reserved_1, queue, exchange, routingKey, arguments) =>
            if (queue != null && queue.startsWith("amp.")) {
              pushConnectionClose(channel.id, ErrorCodes.ACCESS_REFUSED, s"Queue $queue starting with 'amq.' are reserved for internal use by the broker", method.classId, method.id)
            } else {
              val future = (exchangeSharding ? ExchangeEntity.QueueUnbind(vhost, exchange, queue, routingKey, arguments)).mapTo[Boolean] andThen {
                case Success(_) =>
                case Failure(e) => log.error(e, e.getMessage)
              }

              val callback = getAsyncCallback[Boolean] { _ =>
                val method = Queue.UnbindOk
                val command = AMQCommand(channel, method)

                push(out, command.render)
              }

              future.foreach(callback.invoke)
            }

          case Queue.Purge(reserved_1, queue, nowait) =>
            if (queue != null && queue.startsWith("amp.")) {
              pushConnectionClose(channel.id, ErrorCodes.ACCESS_REFUSED, s"Queue $queue starting with 'amq.' are reserved for internal use by the broker", method.classId, method.id)
            } else {
              val future = (queueSharding ? QueueEntity.Purge(vhost, queue, connection.id)).mapTo[(Boolean, Int)] andThen {
                case Success(_) =>
                case Failure(e) => log.error(e, e.getMessage)
              }

              val callback = getAsyncCallback[(Boolean, Int)] {
                case (true, nMsgsPurged) =>
                  val method = Queue.PurgeOk(nMsgsPurged)
                  val command = AMQCommand(channel, method)

                  push(out, command.render)
                case (false, _) =>
                  pushConnectionClose(0, ErrorCodes.NOT_ALLOWED, s"Queue $queue is exclusive and is not allowed to access from another connetion", method.classId, method.id)
              }

              future.foreach(callback.invoke)
            }

          case Queue.Delete(reserved_1, queue, ifUnused, ifEmpty, nowait) =>
            if (queue != null && queue.startsWith("amp.")) {
              pushConnectionClose(channel.id, ErrorCodes.ACCESS_REFUSED, s"Queue $queue starting with 'amq.' are reserved for internal use by the broker", method.classId, method.id)
            } else {
              val future = (queueSharding ? QueueEntity.PendingDelete(vhost, queue, connection.id)).mapTo[(Boolean, Int)] andThen {
                case Success(_) =>
                case Failure(e) => log.error(e, e.getMessage)
              }

              val callback = getAsyncCallback[(Boolean, Int)] {
                case (true, nMsgsDeleted) =>
                  val method = Queue.DeleteOk(nMsgsDeleted)
                  val command = AMQCommand(channel, method)

                  push(out, command.render)
                case (false, _) =>
                  pushConnectionClose(0, ErrorCodes.NOT_ALLOWED, s"Queue $queue is exclusive and is not allowed to access from another connetion", method.classId, method.id)
              }

              future.foreach(callback.invoke)
            }
        }
      }

      private def receivedBasicMethod(channel: AMQChannel, method: AMQMethod, isLastCommand: Boolean) {
        method match {
          case Basic.Consume(reserved_1, queue, consumerTag, noLocal, noAck, exclusive, nowait, arguments) =>
            val future = (queueSharding ? QueueEntity.ConsumerStarted(vhost, queue, consumerTag, connection.id, channel.id)).mapTo[Boolean] andThen {
              case Success(_) =>
              case Failure(e) => log.error(e, e.getMessage)
            }

            val callback = getAsyncCallback[Boolean] { allowed =>
              if (allowed) {
                channel.addConsumer(AMQConsumer(channel, consumerTag, queue, noAck))
                log.info(s"$id added consumer: $consumerTag")

                val command = AMQCommand(channel, Basic.ConsumeOk(consumerTag))

                push(out, command.render)
              } else {
                pushConnectionClose(0, ErrorCodes.NOT_ALLOWED, s"Queue $queue is exclusive and is not allowed to access from another connetion", method.classId, method.id)
              }
            }

            future.foreach(callback.invoke)

          case Basic.Cancel(consumerTag, nowait) =>
            channel.getConsumer(consumerTag) match {
              case Some(consumer) =>
                val future = (queueSharding ? QueueEntity.ConsumerCancelled(vhost, consumer.queue, consumer.tag, connection.id, channel.id)).mapTo[Boolean] andThen {
                  case Success(_) =>
                  case Failure(e) => log.error(e, e.getMessage)
                }

                val callback = getAsyncCallback[Boolean] { allowed =>
                  if (allowed) {
                    channel.removeConsumer(consumerTag)
                    log.info(s"$id removed consumer: $consumerTag")

                    val command = AMQCommand(channel, Basic.CancelOk(consumerTag))

                    push(out, command.render)
                  } else {
                    pushConnectionClose(0, ErrorCodes.NOT_ALLOWED, s"Queue $consumer.queue is exclusive and is not allowed to access from another connetion", method.classId, method.id)
                  }
                }

                future.foreach(callback.invoke)
              case None =>
            }

          case Basic.Get(reserved_1, queue, autoAck) =>

            val future = (queueSharding ? QueueEntity.Pull(vhost, queue, None, connection.id, channel.id, 1, Long.MaxValue, autoAck)).mapTo[Vector[Long]] flatMap { msgIds =>
              Future.sequence(msgIds map { msgId =>
                (messageSharding ? MessageEntity.Get(msgId.toString)).mapTo[Option[Message]]
              })
            } andThen {
              case Success(_) =>
              case Failure(e) => log.error(e, e.getMessage)
            }

            val callback = getAsyncCallback[Vector[Option[Message]]] {
              case Vector(Some(Message(msgId, header, body, exchange, routingKey, ttl))) =>
                val deliveryMsg = channel.goingToDeliveryMsgs(Vector(msgId), channel.basicGetConsumer, autoAck).head
                if (autoAck) {
                  messageSharding ! MessageEntity.Unrefer(msgId.toString)
                }

                val method = Basic.GetOk(deliveryMsg.deliveryTag, redelivered = deliveryMsg.nDelivery > 1, exchange, routingKey, 1)
                val command = AMQCommand(channel, method, header, body)

                push(out, command.render)

              case _ =>
                val clusterId = ""
                val command = AMQCommand(channel, Basic.GetEmpty(clusterId))

                push(out, command.render)
            }

            future.foreach(callback.invoke)

          case Basic.Qos(prefetchSize, prefetchCount, global) =>
            channel.prefetchCount = if (prefetchCount == 0) Int.MaxValue else prefetchCount
            channel.prefetchSize = if (prefetchSize == 0) Long.MaxValue else prefetchSize
            channel.prefetchGlobal = global

            val command = AMQCommand(channel, Basic.QosOk)

            push(out, command.render)

          case Basic.Reject(deliveryTag, requeue) =>
            receivedNack(channel, deliveryTag, false, requeue, isLastCommand)

          case Basic.Nack(deliveryTag, multiple, requeue) =>
            receivedNack(channel, deliveryTag, multiple, requeue, isLastCommand)

          case Basic.Recover(requeue) =>
            receivedRecover(channel, requeue, isLastCommand)

          case m @ Basic.RecoverAsync(requeue) =>
            log.warning(s"Got $m. Redeliver unacknowledged messages. This method asks the server to redeliver all unacknowledged messages on a specified channel. Zero or more messages may be redelivered. This method is deprecated in favour of the synchronous Recover/Recover-Ok.")
        }
      }

      private def receivedAccessMethod(channel: AMQChannel, method: AMQMethod, isLastCommand: Boolean) {
        method match {
          case m @ Access.Request(realm, exclusive, passive, active, write, read) =>
            log.warning(s"Got $m. From AMQP 0-8 to 0-91, access tickets have been removed. This involves the removal of the Access.Request method, and the deprecation of each and every ticket field in methods that used to require a ticket.  The restrictions on virtual-host names have been removed.")
        }
      }

      private def receivedTxMethod(channel: AMQChannel, method: AMQMethod, isLastCommand: Boolean) {
        method match {
          case Tx.Select =>
            log.info(s"TODO Method not be processed: ${method.getClass.getName}")

          case Tx.Commit =>
            log.info(s"TODO Method not be processed: ${method.getClass.getName}")

          case Tx.Rollback =>
            log.info(s"TODO Method not be processed: ${method.getClass.getName}")
        }
      }

      private def receivedConfirmMethod(channel: AMQChannel, method: AMQMethod, isLastCommand: Boolean) {
        method match {
          case Confirm.Select(nowait) =>
            channel.mode match {
              case AMQChannel.Transaction =>
                pushConnectionClose(channel.id, ErrorCodes.NOT_ALLOWED, s"The channel ${channel.id} is already in transactional mode, cannot be put in confirm mode", method.classId, method.id)
              case AMQChannel.Normal | AMQChannel.Confirm =>
                channel.mode = AMQChannel.Confirm

                val command = AMQCommand(channel, Confirm.SelectOk)

                push(out, command.render)

                log.info(s"$id channel ${channel.id} is put in confirm mode")
            }
        }
      }

    }

  }

}

