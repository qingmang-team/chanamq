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
// rabbitmq-java-client/src/main/java/com/rabbitmq/client/impl/ContentHeaderPropertyWriter.java

package chana.mq.amqp.model

import java.io.IOException
import java.time.Instant

final class ContentHeaderPropertyWriter(writer: ValueWriter) {

  private var flagWord: Int = 0
  private var bitCount: Int = 0

  @throws(classOf[IOException])
  private def emitFlagWord(continuationBit: Boolean) {
    writer.writeShort(if (continuationBit) (flagWord | 1) else flagWord)
    flagWord = 0
    bitCount = 0
  }

  @throws(classOf[IOException])
  def writePresence(present: Boolean) {
    if (bitCount == 15) {
      emitFlagWord(true)
    }

    if (present) {
      val bit = 15 - bitCount
      flagWord = flagWord | (1 << bit)
    }
    bitCount += 1
  }

  @throws(classOf[IOException])
  def finishPresence() {
    emitFlagWord(false)
  }

  @throws(classOf[IOException])
  def writeShortstr(str: String) {
    writer.writeShortstr(str)
  }

  @throws(classOf[IOException])
  def writeLongstr(str: String) {
    writer.writeLongstr(str)
  }

  @throws(classOf[IOException])
  def writeLongstr(str: LongString) {
    writer.writeLongstr(str)
  }

  @throws(classOf[IOException])
  def writeShort(s: Int) {
    writer.writeShort(s)
  }

  @throws(classOf[IOException])
  def writeLong(l: Int) {
    writer.writeLong(l)
  }

  @throws(classOf[IOException])
  def writeLonglong(ll: Long) {
    writer.writeLonglong(ll)
  }

  @throws(classOf[IOException])
  def writeTable(table: Map[String, Any]) {
    writer.writeTable(table)
  }

  @throws(classOf[IOException])
  def writeOctet(octet: Int) {
    writer.writeOctet(octet)
  }

  @throws(classOf[IOException])
  def writeTimestamp(timestamp: Instant) {
    writer.writeTimestamp(timestamp)
  }
}
