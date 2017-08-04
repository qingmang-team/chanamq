/**
 * Copyright (C) 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 *
 * @see akka.http.impl.util
 */

package chana.mq.amqp

import akka.ConfigurationException
import akka.actor.ActorContext
import akka.actor.ActorRefFactory
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.io.Inet
import akka.io.Inet.SocketOption
import akka.io.Tcp
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigFactory._
import java.net.InetSocketAddress
import java.net.InetAddress
import scala.collection.immutable
import scala.collection.immutable
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

object Settings {

  implicit def enhanceConfig(config: Config) = new Settings.EnhancedConfig(config)

  private[amqp] class EnhancedConfig(val underlying: Config) extends AnyVal {

    def getPotentiallyInfiniteDuration(path: String): Duration = underlying.getString(path) match {
      case "infinite" => Duration.Inf
      case x          => Duration(x)
    }

    def getFiniteDuration(path: String): FiniteDuration = Duration(underlying.getString(path)) match {
      case x: FiniteDuration => x
      case _                 => throw new ConfigurationException(s"Config setting '$path' must be a finite duration")
    }

    def getPossiblyInfiniteInt(path: String): Int = underlying.getString(path) match {
      case "infinite" => Int.MaxValue
      case x          => underlying.getInt(path)
    }

    def getIntBytes(path: String): Int = {
      val value: Long = underlying getBytes path
      if (value <= Int.MaxValue) value.toInt
      else throw new ConfigurationException(s"Config setting '$path' must not be larger than ${Int.MaxValue}")
    }

    def getPossiblyInfiniteIntBytes(path: String): Int = underlying.getString(path) match {
      case "infinite" => Int.MaxValue
      case x          => getIntBytes(path)
    }

    def getPossiblyInfiniteBytes(path: String): Long = underlying.getString(path) match {
      case "infinite" => Long.MaxValue
      case x          => underlying.getBytes(path)
    }
  }

  private def actorSystem(implicit refFactory: ActorRefFactory): ExtendedActorSystem =
    refFactory match {
      case x: ActorContext        => actorSystem(x.system)
      case x: ExtendedActorSystem => x
      case _                      => throw new IllegalStateException
    }

  lazy val configAdditions: Config = {
    val localHostName = try {
      new InetSocketAddress(InetAddress.getLocalHost, 9402).getHostString
    } catch {
      case NonFatal(_) => ""
    }

    ConfigFactory.parseMap(Map("chana.amqp.hostname" -> localHostName).asJava)
  }

}
private[amqp] abstract class Settings[T](protected val prefix: String) {
  private final val MaxCached = 8
  private[this] var cache = immutable.ListMap.empty[ActorSystem, T]

  implicit def enhanceConfig(config: Config) = new Settings.EnhancedConfig(config)

  implicit def default(implicit refFactory: ActorRefFactory): T =
    apply(Settings.actorSystem)

  def apply(system: ActorSystem): T =
    // we use and update the cache without any synchronization,
    // there are two possible "problems" resulting from this:
    // - cache misses of things another thread has already put into the cache,
    //   in these cases we do double work, but simply accept it
    // - cache hits of things another thread has already dropped from the cache,
    //   in these cases we avoid double work, which is nice
    cache.getOrElse(system, {
      val settings = apply(system.settings.config)
      val c =
        if (cache.size < MaxCached) cache
        else cache.tail // drop the first (and oldest) cache entry
      cache = c.updated(system, settings)
      settings
    })

  def apply(configOverrides: String): T =
    apply(parseString(configOverrides)
      .withFallback(Settings.configAdditions)
      .withFallback(defaultReference(getClass.getClassLoader)))

  def apply(config: Config): T =
    fromSubConfig(config, config getConfig prefix)

  def fromSubConfig(root: Config, c: Config): T
}

private[amqp] object SocketOptionSettings {
  import Settings._

  def fromSubConfig(root: Config, c: Config): immutable.Seq[SocketOption] = {
    def so[T](setting: String)(f: (Config, String) => T)(cons: T => SocketOption): List[SocketOption] =
      c.getString(setting) match {
        case "undefined" => Nil
        case x           => cons(f(c, setting)) :: Nil
      }

    so("so-receive-buffer-size")(_ getIntBytes _)(Inet.SO.ReceiveBufferSize) :::
      so("so-send-buffer-size")(_ getIntBytes _)(Inet.SO.SendBufferSize) :::
      so("so-reuse-address")(_ getBoolean _)(Inet.SO.ReuseAddress) :::
      so("so-traffic-class")(_ getInt _)(Inet.SO.TrafficClass) :::
      so("tcp-keep-alive")(_ getBoolean _)(Tcp.SO.KeepAlive) :::
      so("tcp-oob-inline")(_ getBoolean _)(Tcp.SO.OOBInline) :::
      so("tcp-no-delay")(_ getBoolean _)(Tcp.SO.TcpNoDelay)
  }
}

object ServerSettings extends Settings[ServerSettings]("chana.mq.amqp.server") {
  implicit def timeoutsShortcut(s: ServerSettings): ServerSettings.Timeouts = s.timeouts

  /** INTERNAL API */
  final case class Timeouts(
      idleTimeout:    Duration,
      requestTimeout: Duration,
      bindTimeout:    FiniteDuration
  ) {
    require(idleTimeout > Duration.Zero, "idleTimeout must be infinite or > 0")
    require(requestTimeout > Duration.Zero, "requestTimeout must be infinite or > 0")
    require(bindTimeout > Duration.Zero, "bindTimeout must be > 0")
  }

  def fromSubConfig(root: Config, c: Config) = ServerSettings(
    new Timeouts(
      c getPotentiallyInfiniteDuration "idle-timeout",
      c getPotentiallyInfiniteDuration "request-timeout",
      c getFiniteDuration "bind-timeout"
    ),
    c getInt "max-connections",
    c getInt "pipelining-limit",
    c getBoolean "verbose-error-messages",
    c getInt "backlog",
    SocketOptionSettings.fromSubConfig(root, c.getConfig("socket-options"))
  )
}
final case class ServerSettings(
    timeouts:             ServerSettings.Timeouts,
    maxConnections:       Int,
    pipeliningLimit:      Int,
    verboseErrorMessages: Boolean,
    backlog:              Int,
    socketOptions:        immutable.Seq[SocketOption]
) {

  require(0 < maxConnections, "max-connections must be > 0")
  require(0 < pipeliningLimit && pipeliningLimit <= 1024, "pipelining-limit must be > 0 and <= 1024")
  require(0 < backlog, "backlog must be > 0")

  override def productPrefix = "ServerSettings"
}

object ClientSettings extends Settings[ClientSettings]("chana.mq.amqp.client") {

  def fromSubConfig(root: Config, inner: Config) = {
    val c = inner.withFallback(root.getConfig(prefix))
    new ClientSettings(
      connectingTimeout = c getFiniteDuration "connecting-timeout",
      idleTimeout = c getPotentiallyInfiniteDuration "idle-timeout",
      socketOptions = SocketOptionSettings.fromSubConfig(root, c.getConfig("socket-options")),
      localAddress = None
    )
  }
}
final case class ClientSettings(
    connectingTimeout: FiniteDuration,
    idleTimeout:       Duration,
    socketOptions:     immutable.Seq[SocketOption],
    localAddress:      Option[InetSocketAddress]
) {

  require(connectingTimeout >= Duration.Zero, "connectingTimeout must be >= 0")

  override def productPrefix = "ClientSettings"

  def withConnectingTimeout(newValue: FiniteDuration): ClientSettings = this.copy(connectingTimeout = newValue)
  def withIdleTimeout(newValue: Duration): ClientSettings = this.copy(idleTimeout = newValue)
  def withSocketOptions(newValue: immutable.Seq[SocketOption]): ClientSettings = this.copy(socketOptions = newValue)
  def withLocalAddress(newValue: Option[InetSocketAddress]): ClientSettings = this.copy(localAddress = newValue)

  /**
   * Returns a new instance with the given local address set if the given override is `Some(address)`, otherwise
   * return this instance unchanged.
   */
  def withLocalAddressOverride(overrideLocalAddressOption: Option[InetSocketAddress]): ClientSettings =
    if (overrideLocalAddressOption.isDefined) withLocalAddress(overrideLocalAddressOption)
    else this
}
