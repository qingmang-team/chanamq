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
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

object FrameStage {

  type AMQMethod = AMQClass#Method

  def createStage()(implicit system: ActorSystem): GraphStage[FlowShape[ByteString, ByteString]] = {
    new HandlerStage()
  }

  final class HandlerStage()(implicit system: ActorSystem) extends GraphStage[FlowShape[ByteString, ByteString]] {
    import system.dispatcher
    private implicit val timeout: Timeout = system.settings.config.getInt("chana.mq.internal.timeout").seconds

    private val log = Logging(system, this.getClass)

    private val connection = {
      val conn = new AMQConnection()
      val connConfig = system.settings.config.getConfig("chana.mq.connection")

      conn.channelMax = connConfig.getInt("channel-max")
      conn.frameMax = connConfig.getInt("frame-max")
      conn.frameMin = connConfig.getInt("frame-min")

      val heartbeatDelay = connConfig.getInt("heartbeat")
      conn.writerHeartbeat = heartbeatDelay.seconds
      conn.readerHeartbeat = (heartbeatDelay * 2).seconds

      conn
    }
    private val channel0 = new AMQChannel(connection, 0)

    private var virtualHost: VirtualHost = _
    private val channels = mutable.Map[Int, AMQChannel]()

    val in = Inlet[ByteString]("tcp-in")
    val out = Outlet[ByteString]("tcp-out")

    override val shape = new FlowShape[ByteString, ByteString](in, out)

    private val id = System.identityHashCode(HandlerStage.this)

    private val unackedMessageToConsumer = new mutable.HashMap[Long, AMQConsumer]()
    private val channelsContainConsumer = mutable.Set[AMQChannel]()
    private def addConsumer(channel: AMQChannel, consumer: AMQConsumer) {
      if (channel.consumers.isEmpty) {
        channelsContainConsumer += channel
      }
      channel.consumers enqueue consumer

    }

    private def removeConsumer(channel: AMQChannel, consumerTag: String) {
      channel.consumers.dequeueFirst(_.consumerTag == consumerTag)
      if (channel.consumers.isEmpty) {
        channelsContainConsumer -= channel
      }
    }

    private def getConsumer(channel: AMQChannel, consumerTag: String): Option[AMQConsumer] = {
      channel.consumers.find(_.consumerTag == consumerTag)
    }

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
          val command = AMQCommand(method)
          val resp = channel0.render(command)

          push(out, resp)

          // do not call completeStage() here, the client may send back CloseOk or sth? 
          // and cause error: Bad frame end marker: 0
        }

        future.foreach(callback.invoke)
      }

      private def completeAndCloseAllChannels(): Future[Iterable[Boolean]] = {
        Future.sequence(channels.values map completeAndCloseChannel) flatMap { _ =>
          Future.sequence(connection.exclusiveQueues map { queue =>
            (queueSharding ? QueueEntity.ForceDelete(queue, connection.id)).mapTo[Boolean]
          })
        }
      }

      private def completeAndCloseChannel(channel: AMQChannel): Future[mutable.Queue[Boolean]] = {
        Future.sequence(channel.consumers map { consumer => (queueSharding ? QueueEntity.ConsumerCancelled(consumer.queue, connection.id)).mapTo[Boolean] }) map { done =>
          channel.consumers.clear
          channels -= channel.channelNumber
          channelsContainConsumer -= channel
          done
        }
      }

      private class HandshakeOutHandler extends OutHandler {

        override def onPull() {
          //log.info(s"$id outport: onPull ==>")
          pull(in)
        }
      }

      private class HandshakeInHandler extends InHandler {
        var stash = ByteString.empty

        override def onUpstreamFinish() {
          completeStage()
        }

        override def onPush() {
          //log.info(s"$id ==> inport: onPush")
          stash ++= grab(in)
          log.debug(s"stash: $stash")
          if (stash.length >= AMQP.LENGTH_OF_PROTOCAL_HEADER) {
            val (header, rest) = stash.splitAt(AMQP.LENGTH_OF_PROTOCAL_HEADER)
            header.toArray match {
              case Array(Frame.TICK, 0, 0, 0, 0, 0, 0, -50) =>
                // ignore it
                pull(in)

              case Array(Frame.DISCONNECT, 0, 0, 0, 0, 0, 0, -50) =>
                completeStage()

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

                val command = AMQCommand(method)
                val resp = channel0.render(command)

                push(out, resp)

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

      private class FrameOutHandler extends OutHandler {

        /**
         * Is called when the output port is ready to emit the next element,
         * push(out, elem) is now allowed to be called on this port.
         */
        override def onPull() {
          //log.info(s"$id outport: onPull ==>")
          pull(in)
        }
      }

      private class FrameInHandler extends InHandler {
        val frameParser = new FrameParser()
        var commandAssembler = new CommandAssembler()

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

          val data = grab(in)
          val frames = frameParser.onReceive(data.iterator).iterator

          var publishCommands = Vector[AMQCommand[Basic.Publish]]()
          var ackCommands = Vector[AMQCommand[Basic.Ack]]()
          var otherCommands = Vector[AMQCommand[AMQMethod]]()
          var lastCommandOnThisPush: Option[AMQCommand[AMQMethod]] = None

          var shouldClose = false
          while (frames.hasNext && !shouldClose) {
            frames.next() match {
              case FrameParser.Ok(Frame(Frame.TICK, _, _)) =>

              case FrameParser.Ok(Frame(Frame.DISCONNECT, _, _)) =>
                log.info(s"$id Tcp upstream finished, going to close all channels")
                completeAndCloseAllChannels()
                completeStage()
                shouldClose = true

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
                    commandAssembler = new CommandAssembler()
                  case error @ CommandAssembler.Error(frame, reason) =>
                    pushConnectionClose(frame.channel, ErrorCodes.FRAME_ERROR, reason, 0, 0)
                    log.error(error.toString)
                    shouldClose = true

                  case _ => // Go on - wait for next frame
                }

              case error @ FrameParser.Error(errorCode, message) =>
                pushConnectionClose(0, errorCode, message, 0, 0)
                log.error(error.toString)
                shouldClose = true
            }
          }

          if (!shouldClose) {
            lastCommandOnThisPush match {
              case None =>
                pushHeatbeatOrMessagesOrPull()

              case Some(last) =>
                if (otherCommands.length > 1) {
                  log.warning(s"$id Got batch other commands: $otherCommands")
                }
                receivedCommands(otherCommands, last)

                if (publishCommands.nonEmpty) {
                  log.debug(s"$id got publish commands: ${publishCommands.size}")
                  receivedPublishes(publishCommands, isLastCommand = last.method.isInstanceOf[Basic.Publish])
                }

                if (ackCommands.nonEmpty) {
                  log.debug(s"$id got ack commands: ${ackCommands.size}")
                  receivedAcks(ackCommands, isLastCommand = last.method.isInstanceOf[Basic.Ack])
                }
            }
          }
        }

        private def pushHeatbeatOrMessagesOrPull() = {
          if (isHeartbeatTime) {
            pushHeartbeat()
            log.info(s"$id pushed heartbeat")
            isHeartbeatTime = false
          } else if (channelsContainConsumer.nonEmpty) {

            // fair play on consumers for each channel
            val future = Future.sequence(channelsContainConsumer map { channel =>
              log.debug(s"$id $channel has ${channel.consumers}")

              val consumer = channel.consumers.dequeue
              channel.consumers enqueue consumer

              val prefetchCount = if (consumer.autoAck) {
                channel.prefetchCount
              } else {
                val nUnacked = if (channel.prefetchGlobal) {
                  channel.consumers.foldLeft(0) { (acc, consumer) => acc + consumer.nUnacks }
                } else {
                  consumer.nUnacks
                }
                channel.prefetchCount - nUnacked
              }

              log.debug(s"$id prefetchCount: $prefetchCount")
              if (prefetchCount > 0) {
                (queueSharding ? QueueEntity.Pull(consumer.queue, prefetchCount, channel.prefetchSize, consumer.autoAck)).mapTo[Vector[Long]] flatMap { msgIds =>
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

            val callback = getAsyncCallback[mutable.Set[(AMQConsumer, Vector[Long], Vector[Option[Message]])]] { setOfChannelConsumerMsgs =>
              val body = setOfChannelConsumerMsgs.foldLeft(ByteString.newBuilder) {
                case (acc, (consumer @ AMQConsumer(channel, consumerTag, queue, autoAck), msgIds, msgs)) =>
                  log.info(s"$id delivered msgs: ${msgs.size}")
                  if (autoAck) {
                    // Unrefer should happen after message got. Unrefer could be async
                    msgIds foreach { msgId => messageSharding ! MessageEntity.Unrefer(msgId.toString) }
                  } else {
                    msgIds foreach { msgId =>
                      consumer.nUnacks += 1
                      consumer.unackedMessages += msgId
                      unackedMessageToConsumer += (msgId -> consumer)
                    }
                  }

                  msgs.foldLeft(acc) {
                    case (acc, Some(Message(msgId, header, body, exchange, routingKey, ttl))) =>
                      val method = Basic.Deliver(consumerTag, msgId, redelivered = false, exchange, routingKey)
                      val command = AMQCommand(method, header, body)
                      val payload = channel.render(command)

                      acc ++= payload
                    case (acc, None) =>
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

        private def receivedPublishes(publishes: Vector[AMQCommand[Basic.Publish]], isLastCommand: Boolean) {
          log.info(s"$id published msgs: ${publishes.size}")

          // Section 4.7 of the AMQP 0-9-1 core specification explains the 
          // conditions under which ordering is guaranteed: messages published
          // in one channel, passing through one exchange and one queue and
          // one outgoing channel will be received in the same order that
          // they were sent.  
          val future = Future.sequence(publishes.groupBy(x => x.method.exchange) map {
            case (exchange, commands) =>
              val msgIds = idService.nextIds(commands.size)

              Future.sequence((msgIds zip commands) map {
                case (msgId, AMQCommand(publish, header, body, channelId)) =>
                  log.debug(s"Got $msgId, ${try { new String(body.getOrElse(Array())) } catch { case ex: Throwable => "" }}")

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
                    ExchangeEntity.Publish(msgId, publish, body.map(_.length).getOrElse(0), isPersist, ttl)
                  }
              }) flatMap {
                case pubs =>
                  (exchangeSharding ? ExchangeEntity.Publishs(exchange, pubs)).mapTo[Vector[Boolean]] map { undeliverables =>
                    pubs zip undeliverables
                  }
              }
          }) andThen {
            case Success(_) =>
            case Failure(e) => log.error(e, e.getMessage)
          }

          val callback = getAsyncCallback[Iterable[List[(ExchangeEntity.Publish, Boolean)]]] { xs =>
            val undeliverables = xs.flatten.filter(_._2).map(_._1) // TODO check mandatory and immediate
            // if not isLastCommand, means we are still under same onPush (
            // received multiple commands on one push), we should not push 
            // or pull on port until isLastCommand.
            if (isLastCommand) {
              pushHeatbeatOrMessagesOrPull()
            }
          }

          future.foreach(callback.invoke)
        }

        private def receivedAcks(acks: Vector[AMQCommand[Basic.Ack]], isLastCommand: Boolean) {
          log.info(s"$id recv $acks")

          val future = Future.sequence(acks.map(_.method) map {
            case Basic.Ack(deliveryTag, multiple) =>
              unackedMessageToConsumer.get(deliveryTag) match {
                case Some(consumer) if !consumer.autoAck =>
                  if (multiple) {
                    val ackedMsgIds = new mutable.HashSet[Long]()

                    val unackedMsgIds = consumer.unackedMessages.iterator
                    var count = 0
                    var acked = -1L
                    while (unackedMsgIds.hasNext && acked != deliveryTag) {
                      acked = unackedMsgIds.next()
                      count += 1
                      ackedMsgIds += acked
                    }

                    consumer.nUnacks -= count
                    consumer.unackedMessages --= ackedMsgIds
                    unackedMessageToConsumer --= ackedMsgIds
                    log.info(s"unacked: ${consumer.consumerTag} ${consumer.unackedMessages.size}")

                    ackedMsgIds foreach { msgId => messageSharding ! MessageEntity.Unrefer(msgId.toString) }
                    (queueSharding ? QueueEntity.Acked(consumer.queue, ackedMsgIds.toList)).mapTo[Boolean]
                  } else {
                    consumer.nUnacks -= 1
                    consumer.unackedMessages -= deliveryTag
                    unackedMessageToConsumer -= deliveryTag
                    log.info(s"unacked: ${consumer.consumerTag} ${consumer.unackedMessages.size}")

                    messageSharding ! MessageEntity.Unrefer(deliveryTag.toString)
                    (queueSharding ? QueueEntity.Acked(consumer.queue, List(deliveryTag))).mapTo[Boolean]
                  }

                case None =>
                  Future.successful(true)
              }
          }) andThen {
            case Success(_) =>
            case Failure(e) => log.error(e, e.getMessage)
          }

          val callback = getAsyncCallback[Vector[Boolean]] { xs =>
            if (isLastCommand) {
              pushHeatbeatOrMessagesOrPull()
            }
          }

          future.foreach(callback.invoke)
        }

        private def receivedCommands(commands: Vector[AMQCommand[AMQMethod]], lastCommand: AMQCommand[AMQMethod]) {
          log.debug(s"recv commands $commands")
          commands foreach (command => receivedCommand(command, command eq lastCommand))
        }

        private def receivedCommand(command: AMQCommand[AMQMethod], isLastCommand: Boolean) {
          val channelId = command.channelId
          val method = command.method
          log.info(s"$id ${method.className}.${method}")
          isClientExpectingResponse = method.expectResponse

          method.classId match {
            case Basic.`id`      => receivedBasicMethod(channelId, method, isLastCommand)
            case Queue.`id`      => receivedQueueMethod(channelId, method, isLastCommand)
            case Channel.`id`    => receivedChannelMethod(channelId, method, isLastCommand)
            case Exchange.`id`   => receivedExchangeMethod(channelId, method, isLastCommand)
            case Connection.`id` => receivedConnectionMethod(channelId, method, isLastCommand)
            case Access.`id`     => receivedAccessMethod(channelId, method, isLastCommand)
            case Tx.`id`         => receivedTxMethod(channelId, method, isLastCommand)
            case Confirm.`id`    => receivedConfirmMethod(channelId, method, isLastCommand)
          }

          // reset for next command
          isClientExpectingResponse = false
        }

        private def receivedConnectionMethod(channelId: Int, method: AMQMethod, isLastCommand: Boolean) {
          method match {
            case Connection.StartOk(clientProperties, mechanism, response, locale) =>
              if (mechanism == null || mechanism.length == 0) {
                pushConnectionClose(channelId, ErrorCodes.CONNECTION_FORCED, "No Sasl mechanism was specified", method.classId, method.id)
              } else {
                SaslMechanism.getSaslMechanism(mechanism.split(" ")) match {
                  case Some(saslMechanism) =>
                    val x = saslMechanism.handleResponse(response.bytes)
                    log.info(x.toString)

                    val command = AMQCommand(Connection.Tune(connection.channelMax, connection.frameMax, connection.writerHeartbeat.toSeconds.toInt))
                    val resp = channel0.render(command)

                    push(out, resp)
                  case None =>
                    pushConnectionClose(channelId, ErrorCodes.ACCESS_REFUSED, "Sasl mechanism aceess refused", method.classId, method.id)
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

            case Connection.Open(virtualHostName, capabilities, insist) =>
              val virtualHostStr = if ((virtualHostName != null) && virtualHostName.charAt(0) == '/') {
                virtualHostName.substring(1)
              } else {
                virtualHostName
              }

              val virtualHost = VirtualHost(UUID.randomUUID, true)

              if (virtualHost == null) {
                pushConnectionClose(channelId, ErrorCodes.NOT_FOUND, "Unknown virtual host: '" + virtualHostName + "'", method.classId, method.id)
              } else {
                // Check virtualhost access
                if (!virtualHost.isActive) {
                  //String redirectHost = addressSpace.getRedirectHost(getPort());
                  //if(redirectHost != null) {
                  // sendConnectionClose(0, new AMQFrame(0, new ConnectionRedirectBody(getProtocolVersion(), AMQShortString.valueOf(redirectHost), null)));
                  //} else {
                  //sendConnectionClose(ErrorCodes.CONNECTION_FORCED, "Virtual host '" + addressSpace.getName() + "' is not active", 0);
                  //}
                } else {
                  try {
                    HandlerStage.this.virtualHost = virtualHost
                    //setAddressSpace(addressSpace)

                    if (virtualHost.authoriseCreateConnection(connection)) {
                      val command = AMQCommand(Connection.OpenOk(virtualHostName))
                      val resp = channel0.render(command)

                      push(out, resp)
                      //_state = ConnectionState.OPEN
                    } else {
                      pushConnectionClose(channelId, ErrorCodes.ACCESS_REFUSED, "Connection refused", method.classId, method.id)
                    }
                  } catch {
                    case e: Throwable =>
                      log.error(e, e.getMessage)
                      pushConnectionClose(channelId, ErrorCodes.ACCESS_REFUSED, e.getMessage, method.classId, method.id)
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
                val command = AMQCommand(method)
                val resp = channel0.render(command)

                push(out, resp)
                completeStage()
              }

              future.foreach(callback.invoke)

            case Connection.SecureOk(response) =>
              log.info(s"TODO Method not be processed: ${method.getClass.getName}")

            case Connection.CloseOk =>
              log.info(s"TODO Method not be processed: ${method.getClass.getName}")
          }
        }

        private def receivedChannelMethod(channelId: Int, method: AMQMethod, isLastCommand: Boolean) {
          method match {
            case Channel.Open(reserved_1) =>
              //assertState(ConnectionState.OPEN)

              // Protect the broker against out of order frame request.
              val response = if (virtualHost == null) {
                pushConnectionClose(channelId, ErrorCodes.COMMAND_INVALID, "Virtualhost has not yet been set. ConnectionOpen has not been called.", method.classId, method.id)
              } else if (channels.get(channelId).isDefined /*|| channelAwaitingClosure(channelId)*/ ) {
                pushConnectionClose(channelId, ErrorCodes.CHANNEL_ERROR, "Channel " + channelId + " already exists", method.classId, method.id)
              } else if (channelId > connection.channelMax) {
                pushConnectionClose(channelId, ErrorCodes.CHANNEL_ERROR, "Channel " + channelId + " cannot be created as the max allowed channel id is " + connection.channelMax, method.classId, method.id)
              } else {
                val channel = new AMQChannel(connection, channelId)
                channels += (channelId -> channel)

                val command = AMQCommand(Channel.OpenOk(LongString(channelId.toString.getBytes)))
                val resp = channel.render(command)

                push(out, resp)
              }

            case Channel.Close(replyCode, replyText, classId, methodId) =>
              withChannel(channelId, method) { channel =>
                val future = completeAndCloseChannel(channel) andThen {
                  case Success(_) =>
                  case Failure(e) => log.error(e, e.getMessage)
                }

                val callback = getAsyncCallback[mutable.Queue[Boolean]] { _ =>
                  val command = AMQCommand(Channel.CloseOk)
                  val resp = channel.render(command)

                  push(out, resp)
                }

                future.foreach(callback.invoke)
              }

            case Channel.CloseOk =>
              log.info(s"TODO Method not be processed: ${method.getClass.getName}")

            case Channel.Flow(active) =>
              log.info(s"TODO Method not be processed: ${method.getClass.getName}")

            case Channel.FlowOk(active) =>
              log.info(s"TODO Method not be processed: ${method.getClass.getName}")
          }
        }

        private def receivedExchangeMethod(channelId: Int, method: AMQMethod, isLastCommand: Boolean) {
          method match {
            case Exchange.Declare(reserved_1, exchange, tpe, passive, durable, autoDelete, internal, nowait, arguments) =>
              withChannel(channelId, method) { channel =>
                val future = (exchangeSharding ? ExchangeEntity.Existed(exchange)).mapTo[Boolean] flatMap {
                  case true =>
                    log.debug(s"Exchange $exchange existed")
                    Future.successful((true, false))
                  case false =>
                    if (passive) {
                      log.warning(s"Exchange $exchange does not exist when passive declared")
                      Future.successful((false, false))
                    } else {
                      log.debug(s"Exchange $exchange does not exist, will create it")
                      (exchangeSharding ? ExchangeEntity.Declare(exchange, tpe, durable, autoDelete, internal, arguments)).mapTo[Boolean] map (created => (false, created))
                    }
                } andThen {
                  case Success(_) =>
                  case Failure(e) => log.error(e, e.getMessage)
                }

                val callback = getAsyncCallback[(Boolean, Boolean)] {
                  case (existed, created) =>
                    if (passive && !existed) {
                      pushConnectionClose(channelId, ErrorCodes.NOT_FOUND, s"Exchange $exchange does not exists when passive declared", method.classId, method.id)
                    } else {
                      val command = AMQCommand(Exchange.DeclareOk)
                      val resp = channel.render(command)

                      push(out, resp)
                    }
                }

                future.foreach(callback.invoke)
              }

            case Exchange.Delete(reserved_1, exchange, ifUnused, nowait) =>
              withChannel(channelId, method) { channel =>
                val future = (exchangeSharding ? ExchangeEntity.Existed(exchange)).mapTo[Boolean] flatMap {
                  case true =>
                    (exchangeSharding ? ExchangeEntity.Delete(exchange, ifUnused, connection.id)).mapTo[Boolean] map (deleted => (true, deleted))
                  case false =>
                    Future.successful((false, false))
                } andThen {
                  case Success(_) =>
                  case Failure(e) => log.error(e, e.getMessage)
                }

                val callback = getAsyncCallback[(Boolean, Boolean)] {
                  // (existed, deleted)
                  case (true, deleted) =>
                    val command = AMQCommand(Exchange.DeleteOk)
                    val resp = channel.render(command)

                    push(out, resp)
                  case (false, _) =>
                    pushConnectionClose(channelId, ErrorCodes.NOT_FOUND, s"Exchange $exchange does not exists", method.classId, method.id)
                }

                future.foreach(callback.invoke)
              }

            case Exchange.Bind(reserved_1, destination, source, routingKey, nowait, arguments) =>
              log.info(s"TODO Method not be processed: ${method.getClass.getName}")

            case Exchange.Unbind(reserved_1, destination, source, routingKey, nowait, arguments) =>
              log.info(s"TODO Method not be processed: ${method.getClass.getName}")
          }
        }

        private def receivedQueueMethod(channelId: Int, method: AMQMethod, isLastCommand: Boolean) {
          method match {
            case Queue.Declare(reserved_1, queue, passive, durable, exclusive, autoDelete, nowait, arguments) =>
              withChannel(channelId, method) { channel =>
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

                val future = (queueSharding ? QueueEntity.Declare(queueName, durable, exclusive, autoDelete, connection.id, ttl)).mapTo[QueueEntity.Statistics] andThen {
                  case Success(_) =>
                  case Failure(e) => log.error(e, e.getMessage)
                }

                val callback = getAsyncCallback[QueueEntity.Statistics] { statis =>
                  if (passive && !statis.existed) {
                    pushConnectionClose(channelId, ErrorCodes.NOT_FOUND, s"Queue $queueName does not exists when passive declared", method.classId, method.id)
                  } else {
                    val method = Queue.DeclareOk(queueName, statis.queueSize, statis.consumerCount)
                    val command = AMQCommand(method)
                    val resp = channel.render(command)

                    push(out, resp)
                  }
                }

                future.foreach(callback.invoke)
              }

            case Queue.Bind(reserved_1, queue, exchange, routingKey, nowait, arguments) =>
              withChannel(channelId, method) { channel =>
                val future = (exchangeSharding ? ExchangeEntity.QueueBind(exchange, queue, routingKey, arguments)).mapTo[Boolean] andThen {
                  case Success(_) =>
                  case Failure(e) => log.error(e, e.getMessage)
                }

                val callback = getAsyncCallback[Boolean] { _ =>
                  val method = Queue.BindOk
                  val command = AMQCommand(method)
                  val resp = channel.render(command)

                  push(out, resp)
                }

                future.foreach(callback.invoke)
              }

            case Queue.Unbind(reserved_1, queue, exchange, routingKey, arguments) =>
              withChannel(channelId, method) { channel =>
                val future = (exchangeSharding ? ExchangeEntity.QueueUnbind(exchange, queue, routingKey, arguments)).mapTo[Boolean] andThen {
                  case Success(_) =>
                  case Failure(e) => log.error(e, e.getMessage)
                }

                val callback = getAsyncCallback[Boolean] { _ =>
                  val method = Queue.UnbindOk
                  val command = AMQCommand(method)
                  val resp = channel.render(command)

                  push(out, resp)
                }

                future.foreach(callback.invoke)
              }

            case Queue.Purge(reserved_1, queue, nowait) =>
              withChannel(channelId, method) { channel =>
                val future = (queueSharding ? QueueEntity.Purge(queue, connection.id)).mapTo[(Boolean, Int)] andThen {
                  case Success(_) =>
                  case Failure(e) => log.error(e, e.getMessage)
                }

                val callback = getAsyncCallback[(Boolean, Int)] {
                  case (true, nMsgsPurged) =>
                    val method = Queue.PurgeOk(nMsgsPurged)
                    val command = AMQCommand(method)
                    val resp = channel.render(command)

                    push(out, resp)
                  case (false, _) =>
                    pushConnectionClose(0, ErrorCodes.NOT_ALLOWED, s"Queue $queue is exclusive and is not allowed to access from another connetion", method.classId, method.id)
                }

                future.foreach(callback.invoke)
              }

            case Queue.Delete(reserved_1, queue, ifUnused, ifEmpty, nowait) =>
              withChannel(channelId, method) { channel =>
                val future = (queueSharding ? QueueEntity.PendingDelete(queue, connection.id)).mapTo[(Boolean, Int)] andThen {
                  case Success(_) =>
                  case Failure(e) => log.error(e, e.getMessage)
                }

                val callback = getAsyncCallback[(Boolean, Int)] {
                  case (true, nMsgsDeleted) =>
                    val method = Queue.DeleteOk(nMsgsDeleted)
                    val command = AMQCommand(method)
                    val resp = channel.render(command)

                    push(out, resp)
                  case (false, _) =>
                    pushConnectionClose(0, ErrorCodes.NOT_ALLOWED, s"Queue $queue is exclusive and is not allowed to access from another connetion", method.classId, method.id)
                }

                future.foreach(callback.invoke)
              }
          }
        }

        private def receivedBasicMethod(channelId: Int, method: AMQMethod, isLastCommand: Boolean) {
          method match {
            case Basic.Consume(reserved_1, queue, consumerTag, noLocal, noAck, exclusive, nowait, arguments) =>
              withChannel(channelId, method) { channel =>
                val future = (queueSharding ? QueueEntity.ConsumerStarted(queue, connection.id)).mapTo[Boolean] andThen {
                  case Success(_) =>
                  case Failure(e) => log.error(e, e.getMessage)
                }

                val callback = getAsyncCallback[Boolean] { allowed =>
                  if (allowed) {
                    addConsumer(channel, AMQConsumer(channel, consumerTag, queue, noAck))
                    log.info(s"$id added consumer: $consumerTag")

                    val method = Basic.ConsumeOk(consumerTag)
                    val command = AMQCommand(method)
                    val resp = channel.render(command)

                    push(out, resp)
                  } else {
                    pushConnectionClose(0, ErrorCodes.NOT_ALLOWED, s"Queue $queue is exclusive and is not allowed to access from another connetion", method.classId, method.id)
                  }
                }

                future.foreach(callback.invoke)
              }

            case Basic.Cancel(consumerTag, nowait) =>
              withChannel(channelId, method) { channel =>
                getConsumer(channel, consumerTag) match {
                  case Some(consumer) =>
                    val future = (queueSharding ? QueueEntity.ConsumerCancelled(consumer.queue, connection.id)).mapTo[Boolean] andThen {
                      case Success(_) =>
                      case Failure(e) => log.error(e, e.getMessage)
                    }

                    val callback = getAsyncCallback[Boolean] { allowed =>
                      if (allowed) {
                        removeConsumer(channel, consumerTag)
                        log.info(s"$id removed consumer: $consumerTag")

                        val method = Basic.CancelOk(consumerTag)
                        val command = AMQCommand(method)
                        val resp = channel.render(command)

                        push(out, resp)
                      } else {
                        pushConnectionClose(0, ErrorCodes.NOT_ALLOWED, s"Queue $consumer.queue is exclusive and is not allowed to access from another connetion", method.classId, method.id)
                      }
                    }

                    future.foreach(callback.invoke)
                  case None =>
                }
              }

            case Basic.Get(reserved_1, queue, autoAck) =>
              withChannel(channelId, method) { channel =>

                val future = (queueSharding ? QueueEntity.Pull(queue, 1, Long.MaxValue, autoAck)).mapTo[Vector[Long]] flatMap { msgIds =>
                  Future.sequence(msgIds map { msgId =>
                    (messageSharding ? MessageEntity.Get(msgId.toString)).mapTo[Option[Message]]
                  })
                } andThen {
                  case Success(_) =>
                  case Failure(e) => log.error(e, e.getMessage)
                }

                val callback = getAsyncCallback[Vector[Option[Message]]] {
                  case Vector(Some(Message(deliveryId, header, body, exchange, routingKey, ttl))) =>
                    if (autoAck) {
                      messageSharding ! MessageEntity.Unrefer(deliveryId.toString)
                    } else {
                      channel.basicGetConsumer.nUnacks += 1
                      channel.basicGetConsumer.unackedMessages += deliveryId
                      unackedMessageToConsumer += (deliveryId -> channel.basicGetConsumer)
                    }

                    val method = Basic.GetOk(deliveryId, redelivered = false, exchange, routingKey, 1)
                    val command = AMQCommand(method, header, body)
                    val resp = channel.render(command)

                    push(out, resp)

                  case _ =>
                    val clusterId = ""
                    val method = Basic.GetEmpty(clusterId)
                    val command = AMQCommand(method)
                    val resp = channel.render(command)

                    push(out, resp)
                }

                future.foreach(callback.invoke)
              }

            case Basic.Qos(prefetchSize, prefetchCount, global) =>
              withChannel(channelId, method) { channel =>
                channel.prefetchCount = if (prefetchCount == 0) Int.MaxValue else prefetchCount
                channel.prefetchSize = if (prefetchSize == 0) Long.MaxValue else prefetchSize
                channel.prefetchGlobal = global
                val method = Basic.QosOk
                val command = AMQCommand(method)
                val resp = channel.render(command)

                push(out, resp)
              }

            case Basic.Reject(deliveryTag, requeue) =>
              log.info(s"TODO Method not be processed: ${method.getClass.getName}")

            case Basic.Nack(deliveryTag, multiple, requeue) =>
              log.info(s"TODO Method not be processed: ${method.getClass.getName}")

            case Basic.RecoverAsync(requeue) =>
              log.info(s"TODO Method not be processed: ${method.getClass.getName}")

            case Basic.Recover(requeue) =>
              log.info(s"TODO Method not be processed: ${method.getClass.getName}")
          }
        }

        private def receivedAccessMethod(channelId: Int, method: AMQMethod, isLastCommand: Boolean) {
          method match {
            case Access.Request(realm, exclusive, passive, active, write, read) =>
              log.info(s"TODO Method not be processed: ${method.getClass.getName}")
          }
        }

        private def receivedTxMethod(channelId: Int, method: AMQMethod, isLastCommand: Boolean) {
          method match {
            case Tx.Select =>
              log.info(s"TODO Method not be processed: ${method.getClass.getName}")

            case Tx.Commit =>
              log.info(s"TODO Method not be processed: ${method.getClass.getName}")

            case Tx.Rollback =>
              log.info(s"TODO Method not be processed: ${method.getClass.getName}")
          }
        }

        private def receivedConfirmMethod(channelId: Int, method: AMQMethod, isLastCommand: Boolean) {
          method match {
            case Confirm.Select(nowait) =>
              log.info(s"TODO Method not be processed: ${method.getClass.getName}")
          }
        }

        private def withChannel(channelId: Int, method: AMQMethod)(action: AMQChannel => Unit) {
          channels.get(channelId) match {
            case Some(channel) =>
              action(channel)
            case None =>
              pushConnectionClose(channelId, ErrorCodes.CHANNEL_ERROR, s"Channel $channelId does not exist", method.classId, method.id)
          }
        }

      }

    }

  }

}
