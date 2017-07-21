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
// rabbitmq-java-client/src/main/java/com/rabbitmq/client/impl/MethodArgumentReader.java

package chana.mq.amqp.method

import chana.mq.amqp.model.ValueReader
import chana.mq.amqp.model.LongString
import java.io.IOException
import java.time.Instant

final class ArgumentsReader(in: ValueReader) {
  private var bits: Int = _
  private var nextBitMask: Int = _

  clearBits()

  private def clearBits() {
    bits = 0
    nextBitMask = 0x100 // triggers readOctet first time
  }

  @throws(classOf[IOException])
  def readShortstr(): String = {
    clearBits()
    in.readShortstr()
  }

  @throws(classOf[IOException])
  def readLongstr(): LongString = {
    clearBits()
    in.readLongstr()
  }

  @throws(classOf[IOException])
  def readShort(): Int = {
    clearBits()
    in.readShort()
  }

  @throws(classOf[IOException])
  def readLong(): Int = {
    clearBits()
    in.readLong()
  }

  @throws(classOf[IOException])
  def readLonglong(): Long = {
    clearBits()
    in.readLonglong()
  }

  @throws(classOf[IOException])
  def readBit(): Boolean = {
    if (nextBitMask > 0x80) {
      bits = in.readOctet()
      nextBitMask = 0x01
    }

    val result = (bits & nextBitMask) != 0
    nextBitMask = nextBitMask << 1
    result
  }

  @throws(classOf[IOException])
  def readTable(): Map[String, Any] = {
    clearBits()
    in.readTable()
  }

  @throws(classOf[IOException])
  def readOctet(): Int = {
    clearBits()
    in.readOctet()
  }

  @throws(classOf[IOException])
  def readTimestamp(): Instant = {
    clearBits()
    in.readTimestamp()
  }
}
