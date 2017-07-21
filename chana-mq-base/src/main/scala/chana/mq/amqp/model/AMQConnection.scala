package chana.mq.amqp.model

import scala.concurrent.duration._

object AMQConnection {
  trait State
  object State {
    case object INIT extends State
    case object AWAIT_START_OK extends State
    case object AWAIT_SECURE_OK extends State
    case object AWAIT_TUNE_OK extends State
    case object AWAIT_OPEN extends State
    case object OPEN extends State
  }
}
class AMQConnection() {
  lazy val id = System.identityHashCode(this)

  /**
   * The maximum total number of channels that the client will use per connection.
   *
   * If the client specifies a channel max that is higher than the value provided
   * by the server, the server MUST close the connection without attempting a
   * negotiated close. The server may report the error in some fashion to assist
   * implementors.
   * Zero means unlimited
   */
  var channelMax: Int = _

  /**
   * The largest frame size that the client and server will use for the connection.
   * Zero means that the client does not impose any specific limit but may reject
   * very large frames if it cannot allocate resources for them. Note that the
   * frame-max limit applies principally to content frames, where large contents
   * can be broken into frames of arbitrary size.
   *
   * Until the frame-max has been negotiated, both peers MUST accept frames of up
   * to frame-min-size octets large, and the minimum negotiated value for frame-max
   * is also frame-min-size.
   *
   * If the client specifies a frame max that is higher than the value provided
   * by the server, the server MUST close the connection without attempting a
   * negotiated close. The server may report the error in some fashion to assist
   * implementors.
   */
  var frameMax: Int = _
  var frameMin: Int = _

  /**
   * The delay, in seconds, of the connection heartbeat that the client wants.
   * Zero means the client does not want a heartbeat.
   */
  var writerHeartbeat: FiniteDuration = _
  var readerHeartbeat: FiniteDuration = _

  var exclusiveQueues = Set[String]()
}

