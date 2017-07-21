package chana.mq.amqp.server.service

import akka.actor.ActorSystem
import akka.event.LogSource
import akka.event.Logging

object IdGenerator {
  implicit val logSource: LogSource[AnyRef] = new LogSource[AnyRef] {
    def genString(o: AnyRef): String = o.getClass.getName
    override def getClazz(o: AnyRef): Class[_] = o.getClass
  }

  /**
   * id = (timestamp << 22) | (workId << 12) | (sequence)
   *      |--   42 bits   --|--   10 bits  --|-- 12 bits --|
   *
   * timestampBits = 42L
   * maxTimestamp = math.pow(2, timestampBits).toLong - 1 // 4398046511103
   *
   * 4398046511103 ms = 4398046511103 / (366 * 24 * 60 * 60 * 1000) = 2823 years
   *
   * Timestamp 4398046511103 before left shift
   * 0 0 0 0  0 0 0 0  0 0 0 0  0 0 0 0  0 0 0 0  0 0 1 1  1 1 1 1  1 1 1 1
   * <-                   22 bits                  ->
   * 1 1 1 1  1 1 1 1  1 1 1 1  1 1 1 1  1 1 1 1  1 1 1 1  1 1 1 1  1 1 1 1
   */
  private val workerIdBits = 10L
  private val sequenceBits = 12L

  private val workerIdShift = sequenceBits
  private val timestampShift = workerIdBits + sequenceBits

  private val maxWorkerId = math.pow(2, workerIdBits).toLong - 1 // 1023 --- 0011 1111 1111
  private val maxSequenceIdAndMask = math.pow(2, sequenceBits).toLong - 1 // 4093 --- 1111 1111 1111

  def timestampOfId(id: Long): Long = {
    id >> timestampShift
  }

}
final class IdGenerator(val workerId: Long)(system: ActorSystem) {
  import IdGenerator._

  private val log = Logging(system, this)

  private var sequence: Long = 0L
  private var lastTimestamp = -1L

  if (workerId > maxWorkerId || workerId < 0) {
    throw new IllegalArgumentException(s"worker Id $workerId can't be greater than $maxWorkerId or less than 0")
  }

  log.info(s"IdGenerator - $workerId started")

  def nextId(): Long = synchronized {
    var timestamp = System.currentTimeMillis

    if (timestamp > lastTimestamp) {
      sequence = 0
    } else if (timestamp == lastTimestamp) {
      sequence = (sequence + 1) & maxSequenceIdAndMask
      if (sequence == 0) {
        timestamp = tilNextMillis(lastTimestamp)
      }
    } else {
      log.error("Clock is moving backwards. Rejecting requests until %d.", lastTimestamp)
      throw new RuntimeException("Clock is moving backwards. Rejecting requests until %d.".format(lastTimestamp))
    }

    lastTimestamp = timestamp

    (timestamp << timestampShift) | (workerId << workerIdShift) | sequence
  }

  def nextIds(n: Int): List[Long] = synchronized {
    val msgIds = Array.ofDim[Long](n)
    var i = 0
    while (i < n) {
      msgIds(i) = nextId()
      i += 1
    }
    msgIds.toList
  }

  private def tilNextMillis(lastTimestamp: Long): Long = {
    var timestamp = System.currentTimeMillis
    while (timestamp <= lastTimestamp) {
      timestamp = System.currentTimeMillis
    }
    timestamp
  }

}