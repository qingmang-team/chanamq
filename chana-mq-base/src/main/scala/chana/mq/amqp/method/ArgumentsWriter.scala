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
// rabbitmq-java-client/src/main/java/com/rabbitmq/client/impl/MethodArgumentWriter.java

package chana.mq.amqp.method

import chana.mq.amqp.model.LongString
import chana.mq.amqp.model.ValueWriter
import java.io.IOException
import java.time.Instant

final class ArgumentsWriter(out: ValueWriter) {
  private var needBitFlush: Boolean = _
  private var bitAccumulator: Byte = _
  private var bitMask: Int = _

  resetBitAccumulator()

  private def resetBitAccumulator() {
    needBitFlush = false
    bitAccumulator = 0
    bitMask = 1
  }

  @throws(classOf[IOException])
  private def bitflush() {
    if (needBitFlush) {
      out.writeOctet(bitAccumulator)
      resetBitAccumulator()
    }
  }

  @throws(classOf[IOException])
  def writeShortstr(str: String) {
    bitflush()
    out.writeShortstr(str)
  }

  @throws(classOf[IOException])
  def writeLongstr(str: LongString) {
    bitflush()
    out.writeLongstr(str)
  }

  @throws(classOf[IOException])
  def writeLongstr(str: String) {
    bitflush()
    out.writeLongstr(str)
  }

  @throws(classOf[IOException])
  def writeShort(s: Int) {
    bitflush()
    out.writeShort(s)
  }

  @throws(classOf[IOException])
  def writeLong(l: Int) {
    bitflush()
    out.writeLong(l)
  }

  @throws(classOf[IOException])
  def writeLonglong(ll: Long) {
    bitflush()
    out.writeLonglong(ll)
  }

  @throws(classOf[IOException])
  def writeBit(b: Boolean) {
    if (bitMask > 0x80) {
      bitflush()
    }
    if (b) {
      bitAccumulator = (bitAccumulator.toInt | bitMask).toByte
    } else {
      // don't set the bit.
    }
    bitMask = bitMask << 1
    needBitFlush = true
  }

  @throws(classOf[IOException])
  def writeTable(table: Map[String, Any]) {
    bitflush()
    out.writeTable(table)
  }

  @throws(classOf[IOException])
  def writeOctet(octet: Int) {
    bitflush()
    out.writeOctet(octet)
  }

  @throws(classOf[IOException])
  def writeOctet(octet: Byte) {
    bitflush()
    out.writeOctet(octet)
  }

  @throws(classOf[IOException])
  def writeTimestamp(timestamp: Instant) {
    bitflush()
    out.writeTimestamp(timestamp)
  }

  @throws(classOf[IOException])
  def flush() {
    bitflush()
    out.flush()
  }
}
