package chana.mq.amqp.server

package object engine {
  sealed trait Control
  case object Tick extends Control
  case object Disconnect extends Control
}
