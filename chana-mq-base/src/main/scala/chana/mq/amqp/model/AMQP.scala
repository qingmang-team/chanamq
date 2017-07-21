package chana.mq.amqp.model

/**
 * AMQP-0.9.1 section 4.9 Limitations
 * The AMQP specifications impose these limits on future extensions of AMQP or
 * protocols from the same wire-level format:
 *
 * Number of channels per connection: 16-bit channel number.
 * Number of protocol classes: 16-bit class id.
 * Number of methods per protocol class: 16-bit method id.
 * The AMQP specifications impose these limits on data:
 *
 * Maximum size of a short string: 255 octets.
 * Maximum size of a long string or field table: 32-bit size.
 * Maximum size of a frame payload: 32-bit size.
 * Maximum size of a content: 64-bit size.
 *
 * The server or client may also impose its own limits on resources such as
 * number of simultaneous connections, number of consumers per channel, number
 * of queues, etc. These do not affect interoperability and are not specified.
 */
object AMQP {

  object PROTOCOL {
    val MAJOR = 0
    val MINOR = 9
    val REVISION = 1
    val PORT = 5672
  }

  val LENGTH_OF_PROTOCAL_HEADER = 8

  trait ExchangeType { def NAME: String }
  object ExchangeType {
    case object DIRECT extends ExchangeType { val NAME = "direct" }
    case object FANOUT extends ExchangeType { val NAME = "fanout" }
    case object TOPIC extends ExchangeType { val NAME = "topic" }
    case object HEADERS extends ExchangeType { val NAME = "headers" }
    def typeOf(name: String) = {
      name match {
        case DIRECT.NAME  => DIRECT
        case FANOUT.NAME  => FANOUT
        case TOPIC.NAME   => TOPIC
        case HEADERS.NAME => HEADERS
      }
    }
  }
}