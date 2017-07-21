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
// rabbitmq-java-client/src/main/java/com/rabbitmq/client/impl/ValueReader.java

package chana.mq.amqp.model

import java.io.DataInputStream
import java.io.IOException
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant
import scala.collection.mutable

object ValueReader {
  private val INT_MASK = 0xffffffffL

  private def unsignedInt(value: Int): Long = value & INT_MASK

  @throws(classOf[IOException])
  private def readShortstr(in: DataInputStream): String = {
    val bytes = Array.ofDim[Byte](in.readUnsignedByte())
    in.readFully(bytes)
    new String(bytes, "UTF-8")
  }

  @throws(classOf[IOException])
  private def readBytes(in: DataInputStream): Array[Byte] = {
    val len = unsignedInt(in.readInt())
    if (len < Int.MaxValue) {
      val bytes = Array.ofDim[Byte](len.toInt)
      in.readFully(bytes)
      bytes
    } else {
      throw new UnsupportedOperationException(s"Bytes of length great than ${Int.MaxValue} does not supported")
    }
  }

  @throws(classOf[IOException])
  private def readLongstr(in: DataInputStream): LongString = {
    LongString.asLongString(readBytes(in))
  }

  @throws(classOf[IOException])
  private def readTimestamp(in: DataInputStream): Instant = Instant.ofEpochMilli(in.readLong() * 1000)

  @throws(classOf[IOException])
  private def readTable(in: DataInputStream): Map[String, Any] = {
    val lenTable = unsignedInt(in.readInt())
    if (lenTable == 0) return Map()

    var table = Map[String, Any]()
    val tableIn = new DataInputStream(new TruncatedInputStream(in, lenTable))
    while (tableIn.available() > 0) {
      val name = readShortstr(tableIn)
      val value = readFieldValue(tableIn)
      if (!table.contains(name))
        table += (name -> value)
    }
    table
  }

  @throws(classOf[IOException])
  private def readArray(in: DataInputStream): Seq[Any] = {
    val len = unsignedInt(in.readInt())
    val arrayIn = new DataInputStream(new TruncatedInputStream(in, len))
    val array = new mutable.ArrayBuffer[Any]()
    while (arrayIn.available() > 0) {
      val value = readFieldValue(arrayIn)
      array += value
    }
    array
  }

  @throws(classOf[IOException])
  private def readFieldValue(in: DataInputStream): Any = {
    in.readUnsignedByte match {
      case 'S' => readLongstr(in)
      case 'I' => in.readInt()
      case 'D' =>
        val scale = in.readUnsignedByte()
        val unscaled = Array.ofDim[Byte](4)
        in.readFully(unscaled)
        new BigDecimal(new BigInteger(unscaled), scale)
      case 'T' => readTimestamp(in)
      case 'F' => readTable(in)
      case 'A' => readArray(in)
      case 'b' => in.readByte()
      case 'd' => in.readDouble()
      case 'f' => in.readFloat()
      case 'l' => in.readLong()
      case 's' => in.readShort()
      case 't' => in.readBoolean()
      case 'x' => readBytes(in)
      case 'V' => null
      case _ =>
        throw new MalformedFrameException("Unrecognised type in table")
    }
  }

}
final class ValueReader(in: DataInputStream) {

  @throws(classOf[IOException])
  def readShortstr(): String = ValueReader.readShortstr(in)

  @throws(classOf[IOException])
  def readLongstr(): LongString = ValueReader.readLongstr(in)

  @throws(classOf[IOException])
  def readShort(): Int = in.readUnsignedShort()

  @throws(classOf[IOException])
  def readLong(): Int = in.readInt()

  @throws(classOf[IOException])
  def readLonglong(): Long = in.readLong()

  @throws(classOf[IOException])
  def readTable(): Map[String, Any] = ValueReader.readTable(in)

  @throws(classOf[IOException])
  def readOctet(): Int = in.readUnsignedByte()

  @throws(classOf[IOException])
  def readTimestamp(): Instant = ValueReader.readTimestamp(in)
}

import java.io.FilterInputStream
import java.io.IOException
import java.io.InputStream

class TruncatedInputStream(in: InputStream, limit: Long) extends FilterInputStream(in) {

  private var count = 0L
  private var mark = 0L

  @throws(classOf[IOException])
  override def available(): Int = Math.min(limit - count, super.available()).toInt

  @throws(classOf[IOException])
  override def mark(readlimit: Int) = this synchronized {
    super.mark(readlimit)
    mark = count
  }

  @throws(classOf[IOException])
  override def read(): Int = {
    if (count < limit) {
      val result = super.read()
      if (result >= 0) {
        count += 1
      }

      result
    } else {
      -1
    }
  }

  @throws(classOf[IOException])
  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (limit > count) {
      val result = super.read(b, off, Math.min(len, limit - count).toInt)
      if (result > 0) {
        count += result
      }
      result
    } else {
      -1
    }
  }

  @throws(classOf[IOException])
  override def reset() = this synchronized {
    super.reset()
    count = mark
  }

  @throws(classOf[IOException])
  override def skip(n: Long): Long = {
    val result = super.skip(Math.min(n, limit - count))
    count += result
    result
  }
}
