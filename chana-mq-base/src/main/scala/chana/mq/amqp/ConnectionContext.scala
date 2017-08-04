package chana.mq.amqp

import akka.stream.TLSClientAuth
import akka.stream.TLSProtocol.NegotiateNewSession
import com.typesafe.sslconfig.akka.AkkaSSLConfig
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLParameters
import scala.collection.immutable

object ConnectionContext {
  def amqps(
    sslContext:          SSLContext,
    sslConfig:           Option[AkkaSSLConfig]         = None,
    enabledCipherSuites: Option[immutable.Seq[String]] = None,
    enabledProtocols:    Option[immutable.Seq[String]] = None,
    clientAuth:          Option[TLSClientAuth]         = None,
    sslParameters:       Option[SSLParameters]         = None
  ) = new AmqpsConnectionContext(sslContext, sslConfig, enabledCipherSuites, enabledProtocols, clientAuth, sslParameters)

  def amqp() = AmqpConnectionContext
}
trait ConnectionContext {
  def defaultPort: Int
  def isSecure: Boolean
  def sslConfig: Option[AkkaSSLConfig]
}

object AmqpConnectionContext extends ConnectionContext {
  def defaultPort = 9402
  def isSecure = false
  def sslConfig = None
}

final class AmqpsConnectionContext(
    val sslContext:          SSLContext,
    val sslConfig:           Option[AkkaSSLConfig]         = None,
    val enabledCipherSuites: Option[immutable.Seq[String]] = None,
    val enabledProtocols:    Option[immutable.Seq[String]] = None,
    val clientAuth:          Option[TLSClientAuth]         = None,
    val sslParameters:       Option[SSLParameters]         = None
) extends ConnectionContext {
  def defaultPort = 9401
  def isSecure = true

  def firstSession = NegotiateNewSession(enabledCipherSuites, enabledProtocols, clientAuth, sslParameters)
}