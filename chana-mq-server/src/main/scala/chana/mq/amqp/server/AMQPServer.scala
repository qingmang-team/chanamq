package chana.mq.amqp.server

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import chana.mq.amqp.Amqp
import chana.mq.amqp.ConnectionContext
import chana.mq.amqp.ServerSettings
import chana.mq.amqp.entity.ExchangeEntity
import chana.mq.amqp.entity.MessageEntity
import chana.mq.amqp.entity.QueueEntity
import chana.mq.amqp.entity.VhostEntity
import chana.mq.amqp.server.engine.ServerBluePrint
import chana.mq.amqp.server.service.GlobalNodeIdService
import java.nio.file.Files
import java.nio.file.Paths
import java.security.KeyStore
import java.security.SecureRandom
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory

/**
 * raw tcp stream:
 * Flow[ByteString, ByteString, Future[OutgoingConnection]]
 */
object AMQPServer {

  implicit val system = ActorSystem("chanamq")
  implicit val materializer = ActorMaterializer()

  GlobalNodeIdService.start(system, Some("entity"))
  GlobalNodeIdService.startProxy(system, Some("entity"))

  VhostEntity.startSharding(system)
  ExchangeEntity.startSharding(system)
  QueueEntity.startSharding(system)
  MessageEntity.startSharding(system)

  def main(args: Array[String]) {

    val amqpServerConfig = system.settings.config.getConfig("chana.mq.amqp.server")
    val amqpEnable = amqpServerConfig.getBoolean("enable")
    val amqpHost = amqpServerConfig.getString("interface")
    val amqpPort = amqpServerConfig.getInt("port")

    val amqpsServerConfig = system.settings.config.getConfig("chana.mq.amqps.server")
    val amqpsEnable = amqpsServerConfig.getBoolean("enable")
    val amqpsHost = amqpsServerConfig.getString("interface")
    val amqpsPort = amqpsServerConfig.getInt("port")

    val serverSetting = ServerSettings(system)

    if (amqpEnable) {
      Amqp().startServer(amqpHost, amqpPort)(serverLogic)
    }

    if (amqpsEnable) {
      val sslConfig = system.settings.config.getConfig("chana.mq.ssl")

      val password = sslConfig.getString("password").toCharArray
      val keystorePath = Paths.get(sslConfig.getString("keystore"))
      val keystore = Files.newInputStream(keystorePath)

      require(keystore != null, "Keystore required!")
      val ks = KeyStore.getInstance("PKCS12")
      ks.load(keystore, password)

      val kmf = KeyManagerFactory.getInstance("SunX509")
      kmf.init(ks, password)

      val tmf = TrustManagerFactory.getInstance("SunX509")
      tmf.init(ks)

      val sslContext = SSLContext.getInstance("TLS")
      sslContext.init(kmf.getKeyManagers, tmf.getTrustManagers, new SecureRandom())
      val amqps = ConnectionContext.amqps(sslContext)

      Amqp().startServer(amqpsHost, amqpsPort, connectionContext = amqps)(serverLogic)
    }
  }

  def serverLogic(settings: ServerSettings, log: LoggingAdapter)(implicit system: ActorSystem): Flow[ByteString, ByteString, NotUsed] = {
    ServerBluePrint(settings, log)
  }

}
