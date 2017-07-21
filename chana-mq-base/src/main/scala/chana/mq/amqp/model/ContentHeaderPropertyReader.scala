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
// rabbitmq-java-client/src/main/java/com/rabbitmq/client/impl/ContentHeaderPropertyReader.java

package chana.mq.amqp.model

import java.io.IOException
import java.time.Instant

final class ContentHeaderPropertyReader(reader: ValueReader) {

  /**
   * The property flags are an array of bits that indicate the presence or
   * absence of each property value in sequence. The bits are ordered from
   * most high to low - bit 15 indicates the first property.
   */
  private var flagWord: Int = 1

  /**
   * The property flags can specify more than 16 properties. If the last bit (0)
   * is set, this indicates that a further property flags field follows. There
   * are many property flags fields as needed.
   */
  private var bitCount = 15

  private def isContinuationBitSet: Boolean = (flagWord & 1) != 0

  @throws(classOf[IOException])
  def readFlagWord() {
    if (!isContinuationBitSet) {
      throw new IOException("Attempted to read flag word when none advertised")
    }
    flagWord = reader.readShort()
    bitCount = 0
  }

  @throws(classOf[IOException])
  def readPresence(): Boolean = {
    if (bitCount == 15) {
      readFlagWord();
    }

    val bit = 15 - bitCount
    bitCount += 1
    (flagWord & (1 << bit)) != 0
  }

  @throws(classOf[IOException])
  def finishPresence() {
    if (isContinuationBitSet) {
      throw new IOException("Unexpected continuation flag word")
    }
  }

  @throws(classOf[IOException])
  def readShortstr(): String = {
    reader.readShortstr()
  }

  @throws(classOf[IOException])
  def readLongstr(): LongString = {
    reader.readLongstr()
  }

  @throws(classOf[IOException])
  def readShort(): Int = {
    reader.readShort()
  }

  @throws(classOf[IOException])
  def readLong(): Int = {
    reader.readLong()
  }

  @throws(classOf[IOException])
  def readLonglong(): Long = {
    reader.readLonglong()
  }

  @throws(classOf[IOException])
  def readTable(): Map[String, Any] = {
    reader.readTable()
  }

  @throws(classOf[IOException])
  def readOctet(): Int = {
    reader.readOctet()
  }

  @throws(classOf[IOException])
  def readTimestamp(): Instant = {
    reader.readTimestamp()
  }
}
