package chana.mq.amqp.model

object AMQProtocol {
  val LENGTH_OF_PROTOCAL_HEADER = 8

  trait Type { def name: String }
  case object AMQP extends Type { def name = "AMQP" }
  case object HTTP extends Type { def name = "HTTP" }
}

object ProtocolVersion {

  val V_0_10 = ProtocolVersion(0, 10)
  val V_0_91 = ProtocolVersion(0, 91)
  val V_0_9 = ProtocolVersion(0, 9)
  val V_0_8 = ProtocolVersion(0, 8)

  private val versions = Map(
    Array[Byte](0, 10, 0) -> V_0_10,
    Array[Byte](0, 9, 1) -> V_0_91,
    Array[Byte](0, 9, 0) -> V_0_9,
    Array[Byte](0, 8, 0) -> V_0_8
  )

  def get(majorVersion: Byte, actualMinorVersion: Byte, revisionVersion: Byte) =
    versions.get(Array[Byte](majorVersion: Byte, actualMinorVersion: Byte, revisionVersion: Byte))

  private val supportedVersions = versions.values.toSet
}
final case class ProtocolVersion(majorVersion: Byte, minorVersion: Byte) {
  def protocolHeader = Array[Byte]('A', 'M', 'Q', 'P', 0, majorVersion, actualMinorVersion, revisionVersion)

  def actualMinorVersion: Byte = {
    (if (minorVersion > 90) (minorVersion / 10) else minorVersion).toByte
  }

  def revisionVersion: Byte = {
    (if (minorVersion > 90) (minorVersion % 10) else 0).toByte
  }

  def isSupported = ProtocolVersion.supportedVersions.contains(this)

  def compareTo(o: AnyRef): Int = {
    val pv = o.asInstanceOf[ProtocolVersion]

    // 0-8 has it's major and minor numbers the wrong way round (it's actually 8-0)...
    // so we need to deal with that case specially
    if ((majorVersion == 8) && (minorVersion == 0)) {
      val fixedThis = ProtocolVersion(minorVersion, majorVersion)
      return fixedThis.compareTo(pv)
    }

    if ((pv.majorVersion == 8) && (pv.minorVersion == 0)) {
      val fixedOther = ProtocolVersion(pv.minorVersion, pv.majorVersion)
      return this.compareTo(fixedOther)
    }

    if (majorVersion > pv.majorVersion) {
      1
    } else if (majorVersion < pv.majorVersion) {
      -1
    } else if (minorVersion > pv.minorVersion) {
      1
    } else if (minorVersion < pv.minorVersion) {
      -1
    } else {
      0
    }

  }
}