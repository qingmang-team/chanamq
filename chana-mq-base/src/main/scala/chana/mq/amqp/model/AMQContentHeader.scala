package chana.mq.amqp.model

import chana.mq.amqp.method.UnknownClassOrMethodId
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException

object AMQContentHeader {

  def readFrom(payload: Array[Byte]): BasicProperties = {
    val in = new DataInputStream(new ByteArrayInputStream(payload))
    readFrom(in)
  }

  @throws(classOf[IOException])
  def readFrom(in: DataInputStream): BasicProperties = {
    in.readShort() match {
      case 60      => BasicProperties.readFrom(in)
      case classId => throw new UnknownClassOrMethodId(classId)
    }
  }
}
abstract class AMQContentHeader extends Cloneable {

  /**
   * The class-id MUST match the method frame class id
   */
  def classId: Int
  def className: String

  /**
   * The weight field is unused and must be zero.
   */
  def weight: Short = 0
  def bodySize: Long

  @throws(classOf[IOException])
  def writeTo(out: DataOutputStream, bodySize: Long) {
    out.writeShort(weight)
    out.writeLong(bodySize)
    writePropertiesTo(new ContentHeaderPropertyWriter(new ValueWriter(out)))
  }

  @throws(classOf[IOException])
  def writePropertiesTo(writer: ContentHeaderPropertyWriter)

  @throws(classOf[IOException])
  def toFrame(channelNumber: Int, bodySize: Long): Frame = {
    val out = new ByteArrayOutputStream()
    val os = new DataOutputStream(out)
    os.writeShort(classId)
    writeTo(os, bodySize)
    os.flush()
    Frame(Frame.HEADER, channelNumber, out.toByteArray)
  }

  @throws(classOf[CloneNotSupportedException])
  override def clone(): AnyRef = super.clone()
}
