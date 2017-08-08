package chana.mq.amqp

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.annotation.InternalApi
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.stream.Materializer
import akka.stream.TLSClientAuth
import akka.stream.TLSRole
import akka.stream.TLSProtocol._
import akka.stream.scaladsl.BidiFlow
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.TLS
import akka.stream.scaladsl.TLSPlacebo
import akka.stream.scaladsl.Tcp
import akka.util.ByteString
import com.typesafe.config.Config
import com.typesafe.sslconfig.akka.AkkaSSLConfig
import com.typesafe.sslconfig.akka.util.AkkaLoggerFactory
import com.typesafe.sslconfig.ssl.ClientAuth
import com.typesafe.sslconfig.ssl.ConfigSSLContextBuilder
import java.net.InetSocketAddress
import java.util.concurrent.TimeoutException
import javax.net.ssl.SSLContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.control.NoStackTrace

object Amqp extends ExtensionId[AmqpExtention] with ExtensionIdProvider {
  def apply()(implicit system: ActorSystem): AmqpExtention = super.apply(system)
  def lookup() = Amqp
  def createExtension(system: ExtendedActorSystem) = new AmqpExtention(system.settings.config getConfig "chana.mq.amqp")(system)
}
class AmqpExtention(private val config: Config)(implicit val system: ActorSystem) extends Extension with DefaultSSLContextCreation {

  import Amqp._

  override val sslConfig = AkkaSSLConfig(system)
  validateAndWarnAboutLooseSettings()

  // configured default AmqpsContext for the client-side
  // SYNCHRONIZED ACCESS ONLY!
  private[this] var _defaultClientAmqpsConnectionContext: AmqpsConnectionContext = _
  private[this] var _defaultServerConnectionContext: ConnectionContext = _

  // ** SERVER ** //

  private[this] final val DefaultPortForProtocol = -1 // any negative value

  type ServerLogic = (ServerSettings, LoggingAdapter) => Flow[ByteString, ByteString, NotUsed]

  /**
   * fullLayer:
   *
   * {{{
   *                      +------------+             +-------------+
   *  tcpIn   ->     in2 ->            -> out2  in  ->             |
   *                      |  tlsStage  |             | serverStage |
   *  tcpOut  <-    out1 <-            <- in1   out <-             |
   *                      +------------+             +-------------+
   * }}}
   *
   */
  def startServer(
    interface: String, port: Int,
    connectionContext: ConnectionContext = defaultServerAmqpContext,
    settings:          ServerSettings    = ServerSettings(system),
    log:               LoggingAdapter    = system.log
  )(serverLogic: ServerLogic)(implicit mat: Materializer) {
    val connectionSink = Sink.foreach[Tcp.IncomingConnection] { incomingConn =>
      system.log.info(s"Incoming connection from: ${incomingConn.remoteAddress}")

      // we should build a new layer instance for each incomingConnection, so do it here
      val sslTlsLayer = sslTlsStage(connectionContext, TLSRole.server)
      val serverLayer = serverLogic(settings, log)

      val fullLayer = sslTlsLayer.reversed.join(serverLayer)

      incomingConn.handleWith(fullLayer)
    }

    val binding: Future[Tcp.ServerBinding] = tcpBind(interface, choosePort(port, connectionContext), settings).to(connectionSink).run()

    import system.dispatcher
    binding onComplete {
      case Success(b) =>
        system.log.info(s"Server started, listening on: ${b.localAddress}")
      case Failure(e) =>
        system.log.info(s"Server could not be bound to $interface:$port: ${e.getMessage}")
    }
  }

  private def tcpBind(interface: String, port: Int, settings: ServerSettings): Source[Tcp.IncomingConnection, Future[Tcp.ServerBinding]] = {
    Tcp().bind(
      interface,
      port,
      settings.backlog,
      settings.socketOptions,
      halfClose = false,
      idleTimeout = Duration.Inf // we knowingly disable idle-timeout on TCP level, as we handle it explicitly in AMQP itself
    )
    //.map { incoming => 
    //  val newFlow =
    //    incoming.flow
    //      // Prevent cancellation from the Http implementation to reach the TCP streams to prevent
    //      // completion / cancellation race towards TCP streams. See #459.
    //      .via(StreamUtils.delayCancellation(settings.lingerTimeout))
    //  incoming.copy(flow = newFlow)
    //}
  }

  private def choosePort(port: Int, connectionContext: ConnectionContext) = if (port >= 0) port else connectionContext.defaultPort

  /**
   * Gets the current default server-side [[ConnectionContext]] – defaults to plain AMQP.
   * Can be modified using [[setDefaultServerAmqpContext]], and will then apply for servers bound after that call has completed.
   */
  def defaultServerAmqpContext: ConnectionContext =
    synchronized {
      if (_defaultServerConnectionContext == null) {
        _defaultServerConnectionContext = ConnectionContext.amqp()
      }
      _defaultServerConnectionContext
    }

  /**
   * Sets the default server-side [[ConnectionContext]].
   * If it is an instance of [[AmqpsConnectionContext]] then the server will be bound using AMQPS.
   */
  def setDefaultServerAmqpContext(context: ConnectionContext): Unit =
    synchronized {
      _defaultServerConnectionContext = context
    }

  /**
   * Gets the current default client-side [[AmqpsConnectionContext]].
   * Defaults used here can be configured using ssl-config or the context can be replaced using [[setDefaultClientAmqpsContext]]
   */
  def defaultClientAmqpsContext: AmqpsConnectionContext =
    synchronized {
      _defaultClientAmqpsConnectionContext match {
        case null =>
          val ctx = createDefaultClientAmqpsContext()
          _defaultClientAmqpsConnectionContext = ctx
          ctx
        case ctx => ctx
      }
    }

  /**
   * Sets the default client-side [[AmqpsConnectionContext]].
   */
  def setDefaultClientAmqpsContext(context: AmqpsConnectionContext): Unit =
    synchronized {
      _defaultClientAmqpsConnectionContext = context
    }

  /**
   * {{{
   *                     +------+                                                +------+
   *  In1(ByteString)  ~>|      |~> Out1(SslTlsOutbound) ~ In1(SslTlsOutbound) ~>|      |~> Out1(ByteString)
   *                     | bidi |                                                | bidi |
   *  Out2(ByteString) <~|      |<~ In2(SslTlsInbound)   ~ Out2(SslTlsInbound) <~|      |<~ In2(ByteString)
   *                     +------+                                                +------+
   * }}}
   */
  def sslTlsStage(connectionContext: ConnectionContext, role: TLSRole, hostInfo: Option[(String, Int)] = None)(implicit system: ActorSystem): BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] =
    tlsSupport atop tls(connectionContext, role, hostInfo)

  /**
   * {{{
   *                     +------+
   *  In1(ByteString)  ~>|      |~> Out1(SslTlsOutbound)
   *                     | bidi |
   *  Out2(ByteString) <~|      |<~ In2(SslTlsInbound)
   *                     +------+
   * }}}
   */
  private val tlsSupport: BidiFlow[ByteString, SslTlsOutbound, SslTlsInbound, ByteString, NotUsed] =
    BidiFlow.fromFlows(
      Flow[ByteString].map(SendBytes),
      Flow[SslTlsInbound].collect { case SessionBytes(_, bytes) => bytes }
    )

  /**
   * Creates real or placebo SslTls stage based on if ConnectionContext is AMQPS or not.
   *
   * {{{
   *                        +------+
   *  In1(SslTlsOutbound) ~>|      |~> Out1(ByteString)
   *                        | bidi |
   *  Out2(SslTlsInbound) <~|      |<~ In2(ByteString)
   *                        +------+
   * }}}
   */
  def tls(connectionContext: ConnectionContext, role: TLSRole, hostInfo: Option[(String, Int)] = None) =
    connectionContext match {
      case ctx: AmqpsConnectionContext => TLS(ctx.sslContext, ctx.sslConfig, ctx.firstSession, role, hostInfo = hostInfo)
      case other                       => TLSPlacebo() // if it's not AMQPS, we don't enable SSL/TLS
    }
}

/**
 * TLS configuration for an AMQPS server binding or client connection.
 * For the sslContext please refer to the com.typeasfe.ssl-config library.
 * The remaining four parameters configure the initial session that will
 * be negotiated, see [[akka.stream.TLSProtocol.NegotiateNewSession]] for details.
 */
trait DefaultSSLContextCreation {

  protected def system: ActorSystem
  protected def sslConfig: AkkaSSLConfig

  // --- log warnings ---
  private[this] def log = system.log

  def validateAndWarnAboutLooseSettings() = {
    val WarningAboutGlobalLoose = "This is very dangerous and may expose you to man-in-the-middle attacks. " +
      "If you are forced to interact with a server that is behaving such that you must disable this setting, " +
      "please disable it for a given connection instead, by configuring a specific AmqpsConnectionContext " +
      "for use only for the trusted target that hostname verification would have blocked."

    if (sslConfig.config.loose.disableHostnameVerification) {
      log.warning("Detected that Hostname Verification is disabled globally (via ssl-config's akka.ssl-config.loose.disableHostnameVerification) for the Amqp extension! " +
        WarningAboutGlobalLoose)
    }

    if (sslConfig.config.loose.disableSNI) {
      log.warning("Detected that Server Name Indication (SNI) is disabled globally (via ssl-config's akka.ssl-config.loose.disableSNI) for the Amqp extension! " +
        WarningAboutGlobalLoose)
    }
  }
  // --- end of log warnings ---

  def createDefaultClientAmqpsContext(): AmqpsConnectionContext = createClientAmqpsContext(sslConfig)

  // TODO: currently the same configuration as client by default, however we should tune this for server-side apropriately (!)
  // https://github.com/akka/akka-http/issues/55
  def createServerAmqpsContext(sslConfig: AkkaSSLConfig): AmqpsConnectionContext = {
    log.warning("Automatic server-side configuration is not supported yet, will attempt to use client-side settings. " +
      "Instead it is recommended to construct the Servers AmqpsConnectionContext manually (via SSLContext).")
    createClientAmqpsContext(sslConfig)
  }

  def createClientAmqpsContext(sslConfig: AkkaSSLConfig): AmqpsConnectionContext = {
    val config = sslConfig.config

    val log = Logging(system, getClass)
    val mkLogger = new AkkaLoggerFactory(system)

    // initial ssl context!
    val sslContext = if (sslConfig.config.default) {
      log.debug("buildSSLContext: ssl-config.default is true, using default SSLContext")
      sslConfig.validateDefaultTrustManager(config)
      SSLContext.getDefault
    } else {
      // break out the static methods as much as we can...
      val keyManagerFactory = sslConfig.buildKeyManagerFactory(config)
      val trustManagerFactory = sslConfig.buildTrustManagerFactory(config)
      new ConfigSSLContextBuilder(mkLogger, config, keyManagerFactory, trustManagerFactory).build()
    }

    // protocols!
    val defaultParams = sslContext.getDefaultSSLParameters
    val defaultProtocols = defaultParams.getProtocols
    val protocols = sslConfig.configureProtocols(defaultProtocols, config)
    defaultParams.setProtocols(protocols)

    // ciphers!
    val defaultCiphers = defaultParams.getCipherSuites
    val cipherSuites = sslConfig.configureCipherSuites(defaultCiphers, config)
    defaultParams.setCipherSuites(cipherSuites)

    // auth!

    val clientAuth = config.sslParametersConfig.clientAuth match {
      case ClientAuth.Default => None
      case ClientAuth.Want    => Some(TLSClientAuth.Want)
      case ClientAuth.Need    => Some(TLSClientAuth.Need)
      case ClientAuth.None    => Some(TLSClientAuth.None)
    }

    // hostname!
    if (!sslConfig.config.loose.disableHostnameVerification) {
      defaultParams.setEndpointIdentificationAlgorithm("https") // TODO
    }

    new AmqpsConnectionContext(sslContext, Some(sslConfig), Some(cipherSuites.toList), Some(defaultProtocols.toList), clientAuth, Some(defaultParams))
  }
}

/** INTERNAL API */
@InternalApi
private[amqp] object AmqpConnectionIdleTimeoutBidi {
  def apply(idleTimeout: FiniteDuration, remoteAddress: Option[InetSocketAddress]): BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] = {
    val connectionToString = remoteAddress match {
      case Some(addr) => s" on connection to [$addr]"
      case _          => ""
    }
    val ex = new AmqpIdleTimeoutException(
      "AMQP idle-timeout encountered" + connectionToString + ", " +
        "no bytes passed in the last " + idleTimeout + ". " +
        "This is configurable by chana.mq.amqp.[server|client].idle-timeout.", idleTimeout
    )

    val mapError = Flow[ByteString].mapError({ case t: TimeoutException ⇒ ex })

    val toNetTimeout: BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] =
      BidiFlow.fromFlows(
        mapError,
        Flow[ByteString]
      )
    val fromNetTimeout: BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] =
      toNetTimeout.reversed

    fromNetTimeout atop BidiFlow.bidirectionalIdleTimeout[ByteString, ByteString](idleTimeout) atop toNetTimeout
  }

}

class AmqpIdleTimeoutException(msg: String, timeout: FiniteDuration) extends TimeoutException(msg: String) with NoStackTrace
