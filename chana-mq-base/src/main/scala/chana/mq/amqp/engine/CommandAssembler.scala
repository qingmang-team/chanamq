// Copyright (c) 2017-Present ChanaMQ contributors.  All rights reserved.
// Copyright (c) 2007-Present Pivotal Software, Inc.  All rights reserved.
//
// This software, the RabbitMQ Java client library, is triple-licensed under the
// Mozilla Public License 1.1 ("MPL"), the GNU General Public License version 2
// ("GPL") and the Apache License version 2 ("ASL"). For the MPL, please see
// LICENSE-MPL-RabbitMQ. For the GPL, please see LICENSE-GPL2.  For the ASL,
// please see LICENSE-APACHE2.
//
// This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND,
// either express or implied. See the LICENSE file for specific language governing
// rights and limitations of this software.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.

// This file has been modified by ChanaMQ contributors from the original file:
// rabbitmq-java-client/src/main/java/com/rabbitmq/client/impl/CommandAssembler.java

package chana.mq.amqp.engine

import akka.util.ByteString
import chana.mq.amqp.method.AMQClass
import chana.mq.amqp.model.AMQCommand
import chana.mq.amqp.model.AMQContentHeader
import chana.mq.amqp.model.BasicProperties
import chana.mq.amqp.model.Frame
import java.io.IOException

object CommandAssembler {
  sealed trait State
  case object ExpectingMethod extends State
  case object ExpectingHeader extends State
  case object ExpectingBody extends State

  final case class Ok(command: AMQCommand[AMQClass#Method]) extends State
  final case class Error(frame: Frame, reason: String) extends State {
    override def toString = s"Error on received frame: $frame, $reason"
  }
}
final class CommandAssembler() {
  import CommandAssembler._

  private var method: AMQClass#Method = _
  private var contentHeader: BasicProperties = _
  private var body = ByteString.newBuilder
  private var channelId: Int = _

  private var nBodyBytesRemaining = 0L
  private var state: State = ExpectingMethod

  @throws(classOf[IOException])
  def onReceive(frame: Frame): State = {
    state = state match {
      case ExpectingMethod => receivedMethodFrame(frame)
      case ExpectingHeader => receivedHeaderFrame(frame)
      case ExpectingBody   => receivedBodyFrame(frame)
      case _               => throw new IllegalStateException("Bad Command State " + this.state)
    }
    state
  }

  @throws(classOf[IOException])
  private def receivedMethodFrame(frame: Frame): State = {
    frame match {
      case Frame(Frame.METHOD, channel, payload) =>
        channelId = frame.channel
        method = AMQClass.readFrom(payload)
        if (method.hasContent)
          ExpectingHeader
        else
          Ok(AMQCommand(method, None, None, channelId))
      case _ =>
        Error(frame, s"expected type Frame.METHOD")
    }
  }

  @throws(classOf[IOException])
  private def receivedHeaderFrame(frame: Frame): State = {
    frame match {
      case Frame(Frame.HEADER, _, payload) =>
        contentHeader = AMQContentHeader.readFrom(payload)
        nBodyBytesRemaining = contentHeader.bodySize
        if (nBodyBytesRemaining > 0) {
          ExpectingBody
        } else {
          Ok(AMQCommand(method, Some(contentHeader), Some(body.result.toArray), channelId))
        }
      case _ =>
        Error(frame, s"expected type Frame.HEADER")
    }
  }

  private def receivedBodyFrame(frame: Frame): State = {
    frame match {
      case Frame(Frame.BODY, _, payload) =>
        if (payload.length > 0) {
          nBodyBytesRemaining -= payload.length
          body.putBytes(payload)
          if (nBodyBytesRemaining > 0) {
            ExpectingBody
          } else if (nBodyBytesRemaining == 0) {
            Ok(AMQCommand(method, Some(contentHeader), Some(body.result.toArray), channelId))
          } else {
            Error(frame, s"nBodyBytesRemaining $nBodyBytesRemaining < 0")
          }
        } else {
          state
        }
      case _ =>
        Error(frame, s"expected type Frame.BODY")
    }
  }
}
