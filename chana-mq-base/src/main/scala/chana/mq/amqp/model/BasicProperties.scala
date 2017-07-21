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
// rabbitmq-java-client/src/main/java/com/rabbitmq/client/impl/AMQImpl.java

package chana.mq.amqp.model

import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.IOException
import java.time.Instant

object BasicProperties {

  def readFrom(payload: Array[Byte]): BasicProperties = {
    val in = new DataInputStream(new ByteArrayInputStream(payload))
    readFrom(in)
  }

  /**
   * A content header payload has this format:
   * 0          2        4           12               14
   * +----------+--------+-----------+----------------+------------- - -
   * | class-id | weight | body size | property flags | property list...
   * +----------+--------+-----------+----------------+------------- - -
   *   short      short    long long   short            remainder...
   */
  def readFrom(in: DataInputStream): BasicProperties = {
    val weight = in.readShort()
    val bodySize = in.readLong()

    val reader = new ContentHeaderPropertyReader(new ValueReader(in))
    val contentType_present = reader.readPresence()
    val contentEncoding_present = reader.readPresence()
    val headers_present = reader.readPresence()
    val deliveryMode_present = reader.readPresence()
    val priority_present = reader.readPresence()
    val correlationId_present = reader.readPresence()
    val replyTo_present = reader.readPresence()
    val expiration_present = reader.readPresence()
    val messageId_present = reader.readPresence()
    val timestamp_present = reader.readPresence()
    val type_present = reader.readPresence()
    val userId_present = reader.readPresence()
    val appId_present = reader.readPresence()
    val clusterId_present = reader.readPresence()

    reader.finishPresence()

    val contentType = if (contentType_present) reader.readShortstr() else null
    val contentEncoding = if (contentEncoding_present) reader.readShortstr() else null
    val headers = if (headers_present) reader.readTable() else null
    val deliveryMode = if (deliveryMode_present) reader.readOctet().asInstanceOf[java.lang.Integer] else null
    val priority = if (priority_present) reader.readOctet().asInstanceOf[java.lang.Integer] else null
    val correlationId = if (correlationId_present) reader.readShortstr() else null
    val replyTo = if (replyTo_present) reader.readShortstr() else null
    val expiration = if (expiration_present) reader.readShortstr() else null
    val messageId = if (messageId_present) reader.readShortstr() else null
    val timestamp = if (timestamp_present) reader.readTimestamp() else null
    val tpe = if (type_present) reader.readShortstr() else null
    val userId = if (userId_present) reader.readShortstr() else null
    val appId = if (appId_present) reader.readShortstr() else null
    val clusterId = if (clusterId_present) reader.readShortstr() else null

    BasicProperties(
      bodySize,
      contentType,
      contentEncoding,
      headers,
      deliveryMode,
      priority,
      correlationId,
      replyTo,
      expiration,
      messageId,
      timestamp,
      tpe,
      userId,
      appId,
      clusterId
    )
  }

}
final case class BasicProperties private (
    bodySize:        Long,
    contentType:     String,
    contentEncoding: String,
    headers:         Map[String, Any],
    deliveryMode:    java.lang.Integer,
    priority:        java.lang.Integer,
    correlationId:   String,
    replyTo:         String,
    expiration:      String,
    messageId:       String,
    timestamp:       Instant,
    tpe:             String,
    userId:          String,
    appId:           String,
    clusterId:       String
) extends AMQContentHeader {

  val classId = 60
  val className = "basic"

  @throws(classOf[IOException])
  def writePropertiesTo(writer: ContentHeaderPropertyWriter) {
    writer.writePresence(this.contentType != null)
    writer.writePresence(this.contentEncoding != null)
    writer.writePresence(this.headers != null)
    writer.writePresence(this.deliveryMode != null)
    writer.writePresence(this.priority != null)
    writer.writePresence(this.correlationId != null)
    writer.writePresence(this.replyTo != null)
    writer.writePresence(this.expiration != null)
    writer.writePresence(this.messageId != null)
    writer.writePresence(this.timestamp != null)
    writer.writePresence(this.tpe != null)
    writer.writePresence(this.userId != null)
    writer.writePresence(this.appId != null)
    writer.writePresence(this.clusterId != null)

    writer.finishPresence()

    if (this.contentType != null) writer.writeShortstr(this.contentType)
    if (this.contentEncoding != null) writer.writeShortstr(this.contentEncoding)
    if (this.headers != null) writer.writeTable(this.headers)
    if (this.deliveryMode != null) writer.writeOctet(this.deliveryMode)
    if (this.priority != null) writer.writeOctet(this.priority)
    if (this.correlationId != null) writer.writeShortstr(this.correlationId)
    if (this.replyTo != null) writer.writeShortstr(this.replyTo)
    if (this.expiration != null) writer.writeShortstr(this.expiration)
    if (this.messageId != null) writer.writeShortstr(this.messageId)
    if (this.timestamp != null) writer.writeTimestamp(this.timestamp)
    if (this.tpe != null) writer.writeShortstr(this.tpe)
    if (this.userId != null) writer.writeShortstr(this.userId)
    if (this.appId != null) writer.writeShortstr(this.appId)
    if (this.clusterId != null) writer.writeShortstr(this.clusterId)
  }

}