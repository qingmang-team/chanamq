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
// rabbitmq-java-client/src/main/java/com/rabbitmq/client/impl/ValueWriter.java

package chana.mq.amqp.model

import java.io.DataOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.math.BigDecimal
import java.time.Instant

object ValueWriter {
  private val COPY_BUFFER_SIZE = 4096

  @throws(classOf[IOException])
  private def copy(input: InputStream, output: OutputStream) {
    val buffer = Array.ofDim[Byte](COPY_BUFFER_SIZE)
    var biteSize = input.read(buffer)
    while (-1 != biteSize) {
      output.write(buffer, 0, biteSize)
      biteSize = input.read(buffer)
    }
  }

}
final class ValueWriter(out: DataOutputStream) {

  @throws(classOf[IOException])
  def writeShortstr(str: String) {
    val bytes = str.getBytes("UTF-8")
    val length = bytes.length
    if (length > 255) {
      throw new IllegalArgumentException("Short string too long; UTF-8 encoded length = " + length + ", max = 255.")
    }
    out.writeByte(bytes.length)
    out.write(bytes)
  }

  @throws(classOf[IOException])
  def writeLongstr(str: LongString) {
    writeLong(str.length.toInt)
    ValueWriter.copy(str.getInputStream, out)
  }

  @throws(classOf[IOException])
  def writeLongstr(str: String) {
    val bytes = str.getBytes("utf-8")
    writeLong(bytes.length)
    out.write(bytes)
  }

  @throws(classOf[IOException])
  def writeShort(s: Int) {
    out.writeShort(s)
  }

  @throws(classOf[IOException])
  def writeLong(l: Int) {
    out.writeInt(l)
  }

  @throws(classOf[IOException])
  def writeLonglong(ll: Long) {
    out.writeLong(ll)
  }

  @throws(classOf[IOException])
  def writeTable(table: Map[String, Any]) {
    if (table == null) {
      out.writeInt(0)
    } else {
      out.writeInt(Frame.tableSize(table).toInt)
      table foreach {
        case (k, v) =>
          writeShortstr(k)
          writeFieldValue(v)

      }
    }
  }

  @throws(classOf[IOException])
  def writeFieldValue(value: Any) {
    value match {
      case x: String =>
        writeOctet('S')
        writeLongstr(x)
      case x: LongString =>
        writeOctet('S')
        writeLongstr(x)
      case x: Int =>
        writeOctet('I')
        writeLong(x)
      case x: BigDecimal =>
        writeOctet('D')
        writeOctet(x.scale)
        val unscaled = x.unscaledValue
        if (unscaled.bitLength > 32) {
          throw new IllegalArgumentException(s"BigDecimal too large to be encoded: ${unscaled.bitLength} > 32")
        } else {
          writeLong(x.unscaledValue.intValue)
        }
      case x: Instant =>
        writeOctet('T')
        writeTimestamp(x)
      case x: Map[String, Any] @unchecked =>
        writeOctet('F')
        writeTable(x)
      case x: Byte =>
        writeOctet('b')
        out.writeByte(x)
      case x: Double =>
        writeOctet('d')
        out.writeDouble(x)
      case x: Float =>
        writeOctet('f')
        out.writeFloat(x)
      case x: Long =>
        writeOctet('l')
        out.writeLong(x)
      case x: Short =>
        writeOctet('s')
        out.writeShort(x)
      case x: Boolean =>
        writeOctet('t')
        out.writeBoolean(x)
      case x: Array[Byte] =>
        writeOctet('x')
        writeLong(x.length)
        out.write(x)
      case x: Array[_] =>
        writeOctet('A')
        writeArray(x)
      case x: Seq[_] @unchecked =>
        writeOctet('A')
        writeArray(x)
      case null =>
        writeOctet('V')
      case _ =>
        throw new IllegalArgumentException("Invalid value type: " + value.getClass.getName)
    }
  }

  @throws(classOf[IOException])
  def writeArray(values: Seq[Any]) {
    if (values == null) {
      out.write(0)
    } else {
      out.writeInt(Frame.arraySize(values).toInt);
      values foreach writeFieldValue
    }
  }

  @throws(classOf[IOException])
  def writeArray(values: Array[Any]) {
    if (values == null) {
      out.write(0)
    } else {
      out.writeInt(Frame.arraySize(values).toInt)
      values foreach writeFieldValue
    }
  }

  @throws(classOf[IOException])
  def writeOctet(octet: Int) {
    out.writeByte(octet)
  }

  @throws(classOf[IOException])
  def writeOctet(octet: Byte) {
    out.writeByte(octet)
  }

  /**
   * Time stamps are held in the 64-bit POSIX time_t format with an accuracy of one second.
   */
  @throws(classOf[IOException])
  def writeTimestamp(timestamp: Instant) {
    writeLonglong(timestamp.toEpochMilli / 1000)
  }

  @throws(classOf[IOException])
  def flush() {
    out.flush()
  }
}
