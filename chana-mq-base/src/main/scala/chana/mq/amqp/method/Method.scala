package chana.mq.amqp.method

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import chana.mq.amqp.model.Frame
import chana.mq.amqp.model.ValueWriter

object AMQClass {

  @throws(classOf[IOException])
  def readFrom(payload: Array[Byte]): AMQClass#Method = {
    val in = new DataInputStream(new ByteArrayInputStream(payload))
    readFrom(in)
  }

  @throws(classOf[IOException])
  def readFrom(in: DataInputStream): AMQClass#Method = {
    in.readShort() match {
      case 10  => Connection.readFrom(in)
      case 20  => Channel.readFrom(in)
      case 30  => Access.readFrom(in)
      case 40  => Exchange.readFrom(in)
      case 50  => Queue.readFrom(in)
      case 60  => Basic.readFrom(in)
      case 90  => Tx.readFrom(in)
      case 85  => Confirm.readFrom(in)
      case cId => throw new UnknownClassOrMethodId(cId, -1)
    }
  }
}

trait AMQClass {
  /* classId an unsigned short */
  def id: Int
  /* className */
  def name: String

  /**
   * @param methodId an unsigned short
   * @param methodName
   */
  abstract class Method(val id: Int, val name: String) {

    def classId = AMQClass.this.id
    def className = AMQClass.this.name

    def hasContent: Boolean

    def expectResponse: Boolean

    @throws(classOf[IOException])
    def writeArgumentsTo(writer: ArgumentsWriter)

    @throws(classOf[IOException])
    def toFrame(channelNumber: Int): Frame = {
      val bodyOut = new ByteArrayOutputStream()
      val os = new DataOutputStream(bodyOut)
      os.writeShort(classId)
      os.writeShort(id)
      val paramsWriter = new ArgumentsWriter(new ValueWriter(os))
      writeArgumentsTo(paramsWriter)
      paramsWriter.flush()
      Frame(Frame.METHOD, channelNumber, bodyOut.toByteArray)
    }

    protected def nonNull(field: String) = s"Invalid configuration: '$field' must be non-null."
  }
}

object UnknownClassOrMethodId {
  private val NO_METHOD_ID = -1
}
class UnknownClassOrMethodId(classId: Int, methodId: Int) extends IOException {
  def this(classId: Int) = this(classId, UnknownClassOrMethodId.NO_METHOD_ID)

  override def toString() = {
    if (this.methodId == UnknownClassOrMethodId.NO_METHOD_ID) {
      super.toString() + "<" + classId + ">"
    } else {
      super.toString() + "<" + classId + "." + methodId + ">"
    }
  }
}
