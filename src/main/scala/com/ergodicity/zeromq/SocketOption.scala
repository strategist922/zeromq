package com.ergodicity.zeromq

import org.zeromq.ZMQ
import com.twitter.util.Duration
import com.twitter.conversions.time._

/**
 * Marker trait representing the base for all socket options
 */
sealed trait SocketOption

/**
 * Marker trait representing the base for all meta operations for a socket
 * such as the context, listener, socket type and poller dispatcher
 */
sealed trait SocketMeta extends SocketOption

sealed trait SocketConnectOption extends SocketOption {
  def endpoint: String
}

sealed trait PubSubOption extends SocketOption {
  def payload: Seq[Byte]
}

object SocketType {

  abstract case class ZMQSocketType(id: Int)

  object Dealer extends ZMQSocketType(ZMQ.DEALER)

  object Sub extends ZMQSocketType(ZMQ.SUB)
}

case class PollTimeoutDuration(duration: Duration = 100.millis) extends SocketMeta

trait Deserializer {
  def apply(frames: Seq[Frame]): Any
}

case class Bind(endpoint: String) extends SocketConnectOption

case class Subscribe(payload: Seq[Byte]) extends PubSubOption {
  def this(topic: String) = this(topic.getBytes("UTF-8"))
}
object Subscribe {
  def apply(topic: String): Subscribe = new Subscribe(topic)
  val all = Subscribe(Seq.empty)
}

case class Unsubscribe(payload: Seq[Byte]) extends PubSubOption {
  def this(topic: String) = this(topic.getBytes("UTF-8"))
}
object Unsubscribe {
  def apply(topic: String): Unsubscribe = new Unsubscribe(topic)
}