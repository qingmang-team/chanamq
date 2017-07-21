package chana.mq.amqp.server.store.cassandra

import akka.actor.ActorSystem
import akka.event.LogSource
import akka.event.Logging
import com.datastax.driver.core.BoundStatement
import com.datastax.driver.core.HostDistance
import com.datastax.driver.core.PoolingOptions
import com.datastax.driver.core.ProtocolOptions
import com.datastax.driver.core.ProtocolVersion
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.SocketOptions
import com.datastax.driver.core.Statement
import com.datastax.driver.core.policies.ConstantReconnectionPolicy
import com.datastax.driver.core.utils.Bytes
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.nio.ByteBuffer
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import chana.mq.amqp.Exchange
import chana.mq.amqp.Message
import chana.mq.amqp.Msg
import chana.mq.amqp.Queue
import chana.mq.amqp.entity.ExchangeEntity
import chana.mq.amqp.model.BasicProperties
import chana.mq.amqp.server.store
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

/**
 * Note for retrieve ByteBuffer blob:
 * Do NOT use buffer.array(), because it returns the
 * buffer's *backing array*, which is not the same thing as its contents:
 * - not all byte buffers have backing arrays
 * - even then, the backing array might be larger than the buffer's contents
 *
 * The driver provides a utility method that handles those details for you:
 *      byte[] array = Bytes.getArray(buffer);
 *
 */
object CassandraOpService extends CassandraResultSetOps {
  implicit val logSource: LogSource[AnyRef] = new LogSource[AnyRef] {
    def genString(o: AnyRef): String = o.getClass.getName
    override def getClazz(o: AnyRef): Class[_] = o.getClass
  }
}
final class CassandraOpService(system: ActorSystem) extends store.DBOpService {
  import CassandraOpService._
  import system.dispatcher

  private val log = Logging(system, this)

  val cassandraConf = system.settings.config.getConfig("chana.mq.cassandra.pass-through")
  val keySpace = cassandraConf.getString("keyspace")
  val cassandraHosts = cassandraConf.getStringList("hosts")
  val cassandraPort = cassandraConf.getString("port").toInt

  // http://docs.datastax.com/en/developer/java-driver/3.1/manual/socket_options/
  // The goal of setReadTimeoutMillis is to give up on a node if it took longer than
  // these thresholds to reply, on the assumption that there’s probably something 
  // wrong with it. Therefore it should be set higher than the server-side timeouts.
  // 
  // http://docs.datastax.com/en/developer/java-driver/3.1/manual/pooling/
  // If all hosts are busy with a full queue, the request will fail with a NoHostAvailableException.
  val cassantraCluster = com.datastax.driver.core.Cluster.builder()
    .addContactPoints(cassandraHosts.toArray(Array.ofDim[String](cassandraHosts.size)): _*)
    .withPort(cassandraPort)
    .withProtocolVersion(ProtocolVersion.V4)
    .withCompression(ProtocolOptions.Compression.SNAPPY)
    .withSocketOptions(new SocketOptions()
      .setReadTimeoutMillis(12000)
      .setConnectTimeoutMillis(10000))
    .withPoolingOptions(new PoolingOptions()
      .setConnectionsPerHost(HostDistance.LOCAL, 4, 10)
      .setConnectionsPerHost(HostDistance.REMOTE, 4, 10)
      .setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
      .setMaxRequestsPerConnection(HostDistance.REMOTE, 32768))
    .withReconnectionPolicy(new ConstantReconnectionPolicy(3000))
    .build()

  var session = cassantraCluster.connect(keySpace)

  startPoolMonitor()
  private def startPoolMonitor() = {
    val loadBalancingPolicy = cassantraCluster.getConfiguration.getPolicies.getLoadBalancingPolicy
    val poolingOptions = cassantraCluster.getConfiguration().getPoolingOptions

    val scheduled = Executors.newScheduledThreadPool(1)
    scheduled.scheduleAtFixedRate(new Runnable() {
      var nNonHostsTimes = 0
      def run() {
        val state = session.getState
        val hosts = state.getConnectedHosts.iterator
        var nHosts = 0
        while (hosts.hasNext) {
          nHosts += 1
          val host = hosts.next
          val distance = loadBalancingPolicy.distance(host)
          val connections = state.getOpenConnections(host)
          val inFlightQueries = state.getInFlightQueries(host)
          log.info(s"$host connections=$connections, current load=$inFlightQueries max load=${connections * poolingOptions.getMaxRequestsPerConnection(distance)}")
        }
        if (nHosts == 0) {
          nNonHostsTimes += 1
          log.error(s"Non connected hosts for this session now $session")
          if (nNonHostsTimes >= 5) {
            log.warning(s"Too long to wait for hosts to be reconnected, try to create a new session now")
            try {
              session.close
            } catch {
              case ex: Throwable => log.error(ex, ex.getMessage)
            }
            try {
              session = cassantraCluster.connect(keySpace)
            } catch {
              case ex: Throwable => log.error(ex, ex.getMessage)
            }
            nNonHostsTimes == 0
          }
        }
      }
    }, 5, 300, TimeUnit.SECONDS)
  }

  // --- msg

  val insertMsgStmt = session.prepare("INSERT INTO msgs (id, tstamp, header, body, exchange, routing) VALUES (?, ?, ?, ?, ?, ?);")
  val insertMsgTtlStmt = session.prepare("INSERT INTO msgs (id, tstamp, header, body, exchange, routing) VALUES (?, ?, ?, ?, ?, ?) USING TTL ?;")
  val selectMsgStmt = session.prepare("SELECT id, tstamp, header, body, exchange, routing, TTL(exchange) FROM msgs WHERE id = ? ORDER BY tstamp DESC;")
  val deleteMsgStmt = session.prepare("DELETE FROM msgs WHERE id = ?;")

  private def _selectMsg(id: Long) = selectMsgStmt.bind(id.asInstanceOf[AnyRef])
  private def _deleteMsg(id: Long) = deleteMsgStmt.bind(id.asInstanceOf[AnyRef])
  private def _insertMsg(id: Long, timestamp: Long, header: Option[ByteBuffer], body: Option[ByteBuffer], exchange: String, routing: String, ttl: Option[Long]) = {
    val bs = if (ttl.isDefined) {
      new BoundStatement(insertMsgTtlStmt)
    } else {
      new BoundStatement(insertMsgStmt)
    }

    bs.setLong(0, id)
    bs.setLong(1, timestamp)
    header foreach { x => bs.setBytes(2, x) }
    body foreach { x => bs.setBytes(3, x) }
    bs.setString(4, exchange)
    bs.setString(5, routing)

    ttl foreach { x =>
      bs.setInt(6, (x / 1000.0).toInt)
    }

    bs
  }

  // --- queue

  val insertQueueMsgStmt = session.prepare("INSERT INTO queues (id, offset, msgid, size) VALUES (?, ?, ?, ?);")
  val insertQueueMsgTtlStmt = session.prepare("INSERT INTO queues (id, offset, msgid, size) VALUES (?, ?, ?, ?) USING TTL ?;")
  val deleteQueueMsgStmt = session.prepare("DELETE FROM queues WHERE id = ?;")
  val deleteQueueMsgBeforeStmt = session.prepare("DELETE FROM queues WHERE id = ? AND offset <= ?;")

  val selectQueueMsgStmt = session.prepare("SELECT id, offset, msgid, size, TTL(msgid) FROM queues WHERE id = ? ORDER BY offset ASC;")
  val selectQueueMsgAfterOffsetStmt = session.prepare("SELECT id, offset, msgid, size, TTL(msgid) FROM queues WHERE id = ? AND offset > ? ORDER BY offset ASC;")
  // TODO ! select from time needs to create another table with primary key as (id, msgid) and change following as msgid >= ?
  val selectQueueMsgFromTimeStmt = session.prepare("SELECT id, offset, msgid, size, TTL(msgid) FROM queues WHERE id = ? AND offset >= ? ORDER BY offset ASC;")

  val insertQueueMetaStmt = session.prepare("INSERT INTO queue_metas (id, lconsumed, nconsumer, durable, ttl) VALUES (?, ?, ?, ?, ?);")
  val deleteQueueMetaStmt = session.prepare("DELETE FROM queue_metas WHERE id = ?;")
  val selectQueueMetaStmt = session.prepare("SELECT * FROM queue_metas WHERE id = ?;")

  val insertQueueUnackStmt = session.prepare("INSERT INTO queue_unacks (id, msgid) VALUES (?, ?);")
  val deleteQueueUnackStmt = session.prepare("DELETE FROM queue_unacks WHERE id = ? AND msgid = ?;")
  val deleteQueueUnacksStmt = session.prepare("DELETE FROM queue_unacks WHERE id = ?;")
  val selectQueueUnackStmt = session.prepare("SELECT * FROM queue_unacks WHERE id = ?;")

  private def _selectQueueMsg(id: String) = selectQueueMsgStmt.bind(id)
  private def _selectQueueMsgAfterOffset(id: String, offset: Long) = selectQueueMsgAfterOffsetStmt.bind(id, offset.asInstanceOf[AnyRef])
  private def _selectQueueMsgFromTime(id: String, fromTime: Long) = selectQueueMsgFromTimeStmt.bind(id, fromTime.asInstanceOf[AnyRef])
  private def _deleteQueueMsg(id: String) = deleteQueueMsgStmt.bind(id)
  private def _deleteQueueMsgBeforeOffset(id: String, offset: Long) = deleteQueueMsgBeforeStmt.bind(id, offset.asInstanceOf[AnyRef])
  private def _insertQueueMsg(id: String, offset: Long, msgId: Long, size: Int, ttl: Option[Long]) = {
    val bs = if (ttl.isDefined) {
      new BoundStatement(insertQueueMsgTtlStmt)
    } else {
      new BoundStatement(insertQueueMsgStmt)
    }

    bs.setString(0, id)
    bs.setLong(1, offset)
    bs.setLong(2, msgId)
    bs.setInt(3, size)

    ttl foreach { x =>
      bs.setInt(4, (x / 1000.0).toInt)
    }
    bs
  }

  private def _selectQueueMeta(id: String) = selectQueueMetaStmt.bind(id)
  private def _deleteQueueMeta(id: String) = deleteQueueMetaStmt.bind(id)
  private def _insertQueueMeta(id: String, lconsumed: Long, nconsumer: Int, isDurable: Boolean, queueMsgTtl: Option[Long]) = {
    val bs = new BoundStatement(insertQueueMetaStmt)

    bs.setString(0, id)
    bs.setLong(1, lconsumed)
    bs.setInt(2, nconsumer)
    bs.setBool(3, isDurable)

    queueMsgTtl foreach { x =>
      bs.setLong(4, x)
    }

    bs
  }

  private def _selectQueueUnack(id: String) = selectQueueUnackStmt.bind(id)
  private def _deleteQueueUnack(id: String, msgId: Long) = deleteQueueUnackStmt.bind(id, msgId.asInstanceOf[AnyRef])
  private def _deleteQueueUnacks(id: String) = deleteQueueUnacksStmt.bind(id)
  private def _insertQueueUnack(id: String, msgId: Long) = insertQueueUnackStmt.bind(id, msgId.asInstanceOf[AnyRef])

  // --- queue_deleted

  val insertQueueMsgDeletedStmt = session.prepare("INSERT INTO queues_deleted (id, offset, msgid, size) VALUES (?, ?, ?, ?);")
  val insertQueueMsgDeletedTtlStmt = session.prepare("INSERT INTO queues_deleted (id, offset, msgid, size) VALUES (?, ?, ?, ?) USING TTL ?;")
  val insertQueueMetaDeletedStmt = session.prepare("INSERT INTO queue_metas_deleted (id, lconsumed, nconsumer, durable) VALUES (?, ?, ?, ?);")
  val insertQueueUnackDeletedStmt = session.prepare("INSERT INTO queue_unacks_deleted (id, msgid) VALUES (?, ?);")

  private def _insertQueueMsgDeleted(id: String, offset: Long, msgId: Long, size: Int, ttl: Option[Long]) = {
    val bs = if (ttl.isDefined) {
      new BoundStatement(insertQueueMsgDeletedTtlStmt)
    } else {
      new BoundStatement(insertQueueMsgDeletedStmt)
    }

    bs.setString(0, id)
    bs.setLong(1, offset)
    bs.setLong(2, msgId)
    bs.setInt(3, size)

    ttl foreach { x =>
      bs.setInt(4, (x / 1000.0).toInt)
    }

    bs
  }
  private def _insertQueueMetaDeleted(id: String, lconsumed: Long, nconsumer: Int, isDurable: Boolean, queueMsgTtl: Option[Long]) = {
    val bs = new BoundStatement(insertQueueMetaDeletedStmt)

    bs.setString(0, id)
    bs.setLong(1, lconsumed)
    bs.setInt(2, nconsumer)
    bs.setBool(3, isDurable)

    queueMsgTtl foreach { x =>
      bs.setLong(4, x)
    }

    bs
  }
  private def _insertQueueUnackDeleted(id: String, msgId: Long) = insertQueueUnackDeletedStmt.bind(id, msgId.asInstanceOf[AnyRef])

  // --- exchange

  val insertExchangeStmt = session.prepare("INSERT INTO exchanges (id, tpe, durable, autodel, internal, args) VALUES (?, ?, ?, ?, ?, ?);")
  val deleteExchangeStmt = session.prepare("DELETE FROM exchanges WHERE id = ?;")
  val selectExchangeStmt = session.prepare("SELECT * FROM exchanges WHERE id = ?;")

  val insertExchangeBindStmt = session.prepare("INSERT INTO exchange_binds (id, queue, key, args) VALUES (?, ?, ?, ?);")
  val deleteExchangeBindStmt = session.prepare("DELETE FROM exchange_binds WHERE id = ? AND queue = ? AND key = ?;")
  val deleteExchangeBindsStmt = session.prepare("DELETE FROM exchange_binds WHERE id = ?;")
  val deleteExchangeBindsOfQueueStmt = session.prepare("DELETE FROM exchange_binds WHERE id = ? AND queue = ?;")
  val selectExchangeBindStmt = session.prepare("SELECT * FROM exchange_binds WHERE id = ?;")

  private def _selectExchange(id: String) = selectExchangeStmt.bind(id)
  private def _deleteExchange(id: String) = deleteExchangeStmt.bind(id)
  private def _insertExchange(id: String, tpe: String, isDurable: Boolean, isAutoDelete: Boolean, isInternal: Boolean, args: java.util.Map[String, Any]) = {
    val bs = new BoundStatement(insertExchangeStmt)
    bs.setString(0, id)
    bs.setString(1, tpe)
    bs.setBool(2, isDurable)
    bs.setBool(3, isAutoDelete)
    bs.setBool(4, isInternal)
    bs.setMap(5, args)
    bs
  }

  private def _selectExchangeBind(id: String) = selectExchangeBindStmt.bind(id)
  private def _deleteExchangeBind(id: String, queue: String, routingKey: String) = deleteExchangeBindStmt.bind(id, queue, routingKey)
  private def _deleteExchangeBinds(id: String) = deleteExchangeBindsStmt.bind(id)
  private def _deleteExchangeBindsOfQueue(id: String, queue: String) = deleteExchangeBindsOfQueueStmt.bind(id, queue)
  private def _insertExchangeBind(id: String, queue: String, routingKey: String, args: java.util.Map[String, Any]) = {
    val bs = new BoundStatement(insertExchangeBindStmt)
    bs.setString(0, id)
    bs.setString(1, queue)
    bs.setString(2, routingKey)
    bs.setMap(3, args)
    bs
  }

  private def doDeleteQueue(id: String): Future[Unit] = {
    execute(_deleteQueueMeta(id)) flatMap { _ =>
      execute(_deleteQueueMsg(id)) flatMap { _ =>
        execute(_deleteQueueUnacks(id))
      }
    } map (_ => ())
  }

  private def doSelectQueueMeta(id: String): Future[Option[(Long, Int, Boolean, Option[Long])]] = {
    execute(_selectQueueMeta(id)) map { rs =>
      val raws = rs.iterator
      if (raws.hasNext) {
        val raw = raws.next()
        val lastConsumed = raw.getLong("lconsumed")
        val consumerCount = raw.getInt("nconsumer")
        val isDurable = raw.getBool("durable")
        val ttl = if (raw.isNull("ttl")) None else Some(raw.getLong("ttl"))
        Some((lastConsumed, consumerCount, isDurable, ttl))
      } else {
        None
      }
    }
  }

  // TODO 
  private def selectQueueFromTime(id: String, fromTime: Long): Future[_] = {
    execute(_selectQueueMsgFromTime(id, fromTime << 22))
  }

  // --- public APIs

  def insertMessage(id: Long, header: Option[BasicProperties], body: Option[Array[Byte]], exchange: String, routing: String, ttl: Option[Long]): Future[Unit] = {
    val start = System.currentTimeMillis

    val headerBytes = header map { x =>
      val headerOut = new ByteArrayOutputStream()
      val headerOs = new DataOutputStream(headerOut)
      x.writeTo(headerOs, body.map(_.length).getOrElse(0).toLong)
      headerOs.flush()
      ByteBuffer.wrap(headerOut.toByteArray)
    }

    val timestamp = header map { x =>
      x.timestamp match {
        case null => 0L
        case x    => x.toEpochMilli
      }
    } getOrElse (0L)

    execute(_insertMsg(id, timestamp, headerBytes, body map ByteBuffer.wrap, exchange, routing, ttl)) map (_ => ()) andThen {
      case Success(_) =>
        log.info(s"Inserted Msg of $id in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to insert msg of $id")
    }
  }

  def selectMessage(id: Long) = {
    val start = System.currentTimeMillis
    execute(_selectMsg(id)) map { rs =>
      val raws = rs.iterator
      if (raws.hasNext) {
        val raw = raws.next
        val timestamp = raw.getLong("tstamp")
        val header = Option(raw.getBytes("header")) map (bytes => BasicProperties.readFrom(Bytes.getArray(bytes)))
        val body = Option(raw.getBytes("body")) map (bytes => Bytes.getArray(bytes))
        val exchange = raw.getString("exchange")
        val routing = raw.getString("routing")
        val ttl = if (raw.isNull("TTL(exchange)")) None else Some(raw.getInt("TTL(exchange)") * 1000L)
        Some(Message(id, header, body, exchange, routing, ttl))
      } else {
        None
      }
    } andThen {
      case Success(_) =>
        log.info(s"Selected msg of $id in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to select msg of $id")
    }
  }

  def deleteMessage(id: Long) = {
    val start = System.currentTimeMillis
    execute(_deleteMsg(id)) map (_ => ()) andThen {
      case Success(_) =>
        log.info(s"Deleted msg of $id in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to delete msg of $id")
    }
  }

  def insertQueueMeta(id: String, lastConsumed: Long, consumerCount: Int, isDurable: Boolean, queueMsgTtl: Option[Long]) = {
    val start = System.currentTimeMillis
    execute(_insertQueueMeta(id, lastConsumed, consumerCount, isDurable, queueMsgTtl)) map (_ => ()) andThen {
      case Success(_) =>
        log.info(s"Inserted queue meta of $id in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to insert queue meta of $id")
    }
  }

  def insertQueueMsg(id: String, offset: Long, msgId: Long, size: Int, ttl: Option[Long]) = {
    val start = System.currentTimeMillis
    execute(_insertQueueMsg(id, offset, msgId, size, ttl)) map (_ => ()) andThen {
      case Success(_) =>
        log.info(s"Inserted queue of $id in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to insert queue of $id")
    }
  }

  def deleteQueueMsgs(id: String) = {
    val start = System.currentTimeMillis
    execute(_deleteQueueMsg(id)) map (_ => ()) andThen {
      case Success(_) =>
        log.info(s"Inserted queue of $id in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to insert queue of $id")
    }
  }

  def consumedQueueMessages(id: String, lastConsumed: Long, consumerCount: Int, isDurable: Boolean, queueMsgTtl: Option[Long], unacks: Vector[Long]) = {
    val start = System.currentTimeMillis
    if (unacks.isEmpty) {
      Future.sequence(List(
        execute(_insertQueueMeta(id, lastConsumed, consumerCount, isDurable, queueMsgTtl)),
        execute(_deleteQueueMsgBeforeOffset(id, lastConsumed))
      )) map (_ => ())
    } else {
      Future.sequence(List(
        execute(_insertQueueMeta(id, lastConsumed, consumerCount, isDurable, queueMsgTtl)),
        execute(_deleteQueueMsgBeforeOffset(id, lastConsumed)),
        Future.sequence(unacks map { msgId => execute(_insertQueueUnack(id, msgId)) })
      )) map (_ => ())
    } andThen {
      case Success(_) =>
        log.info(s"Inserted queue of $id in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to insert queue of $id")
    }
  }

  def selectQueue(id: String) = {
    val start = System.currentTimeMillis
    doSelectQueueMeta(id) flatMap {
      case Some((lastConsumed, consumerCount, isDurable, queueMsgTtl)) =>
        log.debug(s"$lastConsumed, $consumerCount, $isDurable")
        execute(_selectQueueUnack(id)) map { rs =>
          var unacks = Vector[Long]()
          val raws = rs.iterator
          while (raws.hasNext) {
            val raw = raws.next()
            val msgId = raw.getLong("msgid")
            unacks :+= msgId
          }
          unacks
        } flatMap { unacks =>
          execute(_selectQueueMsgAfterOffset(id, lastConsumed)) map { rs =>
            val now = System.currentTimeMillis
            var msgs = Vector[Msg]()
            val raws = rs.iterator
            while (raws.hasNext) {
              val raw = raws.next()
              val msgId = raw.getLong("msgid")
              val offset = raw.getLong("offset")
              val size = raw.getInt("size")
              val expireTime = if (raw.isNull("TTL(msgid)")) None else Some(now + raw.getInt("TTL(msgid)"))
              msgs :+= Msg(msgId, offset, size, expireTime)
            }
            log.debug(s"$msgs")
            Some(Queue(lastConsumed, consumerCount, isDurable, queueMsgTtl, msgs, unacks))
          }
        }

      case None =>
        Future.successful(None)
    } andThen {
      case Success(_) =>
        log.info(s"Selected queue of $id in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to select queue of $id")
    }
  }

  def forceDeleteQueue(id: String) = {
    val start = System.currentTimeMillis
    doDeleteQueue(id) andThen {
      case Success(_) =>
        log.info(s"Force deleted queue of $id in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to force delete queue of $id")
    }
  }

  def pendingDeleteQueue(id: String) = {
    val start = System.currentTimeMillis
    val copyToDeleted = doSelectQueueMeta(id) flatMap {
      case Some((lastConsumed, consumerCount, isDurable, queueMsgTtl)) =>
        execute(_insertQueueMetaDeleted(id, lastConsumed, consumerCount, isDurable, queueMsgTtl)) flatMap { _ =>
          execute(_selectQueueUnack(id)) flatMap { rs =>
            val unacks = new mutable.ListBuffer[Future[Unit]]()
            val raws = rs.iterator
            while (raws.hasNext) {
              val raw = raws.next()
              val msgId = raw.getLong("msgid")
              unacks += execute(_insertQueueUnackDeleted(id, msgId)).map(_ => ())
            }
            Future.sequence(unacks)
          } flatMap { x =>
            execute(_selectQueueMsgAfterOffset(id, lastConsumed)) flatMap { rs =>
              val msgs = new mutable.ListBuffer[Future[Unit]]()
              val raws = rs.iterator
              while (raws.hasNext) {
                val raw = raws.next()
                val offset = raw.getLong("offset")
                val msgId = raw.getLong("msgid")
                val size = raw.getInt("size")
                val ttl = if (raw.isNull("TTL(msgid)")) None else Some(raw.getInt("TTL(msgid)") * 1000L)
                msgs += execute(_insertQueueMsgDeleted(id, offset, msgId, size, ttl)).map(_ => ())
              }

              Future.sequence(msgs)
            }
          }
        }
      case None =>
        Future.successful(())
    }

    // delete original queue
    copyToDeleted flatMap { _ => doDeleteQueue(id) } andThen {
      case Success(_) =>
        log.info(s"Pending deleted queue of $id in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to pending delete queue of $id")
    }
  }

  def deleteQueueConsumedMsgs(id: String) = {
    val start = System.currentTimeMillis
    doSelectQueueMeta(id) flatMap {
      case Some((lastConsumed, _, _, _)) =>
        execute(_deleteQueueMsgBeforeOffset(id, lastConsumed)) map (_ => ())
      case None =>
        Future.successful(())
    } andThen {
      case Success(_) =>
        log.info(s"Deleted queue consumed messages of $id in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to delete queue consumed messages of $id")
    }
  }

  def insertQueueUnacks(id: String, msgIds: List[Long]) = {
    val start = System.currentTimeMillis
    Future.sequence(msgIds map { msgId =>
      execute(_insertQueueUnack(id, msgId))
    }) map (_ => ()) andThen {
      case Success(_) =>
        log.info(s"Inserted unacks of $id in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to insert unacks of $id")
    }
  }

  def deleteQueueUnack(id: String, msgId: Long) = {
    val start = System.currentTimeMillis
    execute(_deleteQueueUnack(id, msgId)) map (_ => ()) andThen {
      case Success(_) =>
        log.info(s"Deleted unacks of $id in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to delete unacks of $id")
    }
  }

  def insertExchange(id: String, tpe: String, isDurable: Boolean, isAutoDelete: Boolean, isInternal: Boolean, args: java.util.Map[String, Any]) = {
    val start = System.currentTimeMillis
    execute(_insertExchange(id, tpe, isDurable, isAutoDelete, isInternal, args)) map (_ => ()) andThen {
      case Success(_) =>
        log.info(s"Inserted exchange of $id in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to insert exchange of $id")
    }
  }

  def insertExchangeBind(id: String, queue: String, routingKey: String, args: java.util.Map[String, Any]) = {
    val start = System.currentTimeMillis
    execute(_insertExchangeBind(id, queue, routingKey, args)) map (_ => ()) andThen {
      case Success(_) =>
        log.info(s"Inserted exchange of $id in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to insert exchange of $id")
    }
  }

  def selectExchange(id: String): Future[Option[Exchange]] = {
    val start = System.currentTimeMillis
    execute(_selectExchange(id)) map { rs =>
      val raws = rs.iterator
      if (raws.hasNext) {
        val raw = raws.next()
        val tpe = raw.getString("tpe")
        val durable = raw.getBool("durable")
        val autodel = raw.getBool("autodel")
        val internal = raw.getBool("internal")
        val args = raw.getMap("args", classOf[String], classOf[String])
        Some((tpe, durable, autodel, internal, args))
      } else {
        None
      }
    } flatMap {
      case Some((tpe, isDurable, isAutoDelete, isInternal, args)) =>
        execute(_selectExchangeBind(id)) map { rs =>
          val binds = new mutable.ListBuffer[ExchangeEntity.QueueBind]()
          val raws = rs.iterator
          while (raws.hasNext) {
            val raw = raws.next()
            val queue = raw.getString("queue")
            val routingKey = raw.getString("key")
            val args = raw.getMap("args", classOf[String], classOf[String])
            import scala.collection.JavaConverters._
            binds += ExchangeEntity.QueueBind(id, queue, routingKey, args.asScala.toMap)
          }
          Some(Exchange(tpe, isDurable, isAutoDelete, isInternal, args.asScala.toMap, binds.toList))
        }
      case None =>
        Future.successful(None)
    } andThen {
      case Success(_) =>
        log.info(s"Inserted exchange of $id in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to insert exchange of $id")
    }
  }

  def deleteExchangeBind(id: String, queue: String, routingKey: String) = {
    val start = System.currentTimeMillis
    execute(_deleteExchangeBind(id, queue, routingKey)) map (_ => ()) andThen {
      case Success(_) =>
        log.info(s"Deleted exchange bind of $id $queue $routingKey in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to delete exchange bind of $id $queue $routingKey")
    }
  }

  def deleteExchangeBindsOfQueue(id: String, queue: String) = {
    val start = System.currentTimeMillis
    execute(_deleteExchangeBindsOfQueue(id, queue)) map (_ => ()) andThen {
      case Success(_) =>
        log.info(s"Deleted exchange binds of $id $queue in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to delete exchange binds of $id $queue")
    }
  }

  def deleteExchange(id: String) = {
    val start = System.currentTimeMillis
    execute(_deleteExchange(id)) flatMap { _ =>
      execute(_deleteExchangeBinds(id))
    } map (_ => ()) andThen {
      case Success(_) =>
        log.info(s"Deleted exchange of $id in ${System.currentTimeMillis - start}ms")
      case Failure(ex) =>
        log.error(ex, s"Failed to delete exchange of $id")
    }
  }

  private def execute(stmt: Statement): Future[ResultSet] = {
    Future.successful(session.execute(stmt))
  }
}
