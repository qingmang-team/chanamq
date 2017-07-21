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
// rabbitmq-java-client/src/main/java/com/rabbitmq/client/impl/LongStringHelper.java

package chana.mq.amqp.model

import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.IOException
import java.io.UnsupportedEncodingException
import java.util.Arrays

object LongString {
  val MAX_LENGTH = 0xffffffffL

  def asLongString(string: String): LongString = {
    if (string == null) {
      null
    } else {
      try {
        LongString(string.getBytes("utf-8"))
      } catch {
        case e: UnsupportedEncodingException =>
          throw new Error("utf-8 encoding support required")
      }
    }
  }

  def asLongString(bytes: Array[Byte]): LongString = {
    if (bytes == null) {
      null
    } else {
      LongString(bytes)
    }
  }
}
final case class LongString(bytes: Array[Byte]) {
  override def equals(o: Any) = {
    o match {
      case x: LongString => Arrays.equals(this.bytes, x.bytes)
      case _             => false
    }
  }

  override def hashCode() = Arrays.hashCode(bytes)

  def length: Long = bytes.length

  @throws(classOf[IOException])
  def getInputStream(): DataInputStream = {
    new DataInputStream(new ByteArrayInputStream(bytes))
  }

  override def toString() = {
    try {
      new String(bytes, "utf-8")
    } catch {
      case e: UnsupportedEncodingException =>
        throw new Error("utf-8 encoding support required");
    }
  }
}

