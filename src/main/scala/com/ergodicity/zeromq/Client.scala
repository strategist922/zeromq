package com.ergodicity.zeromq

import com.twitter.concurrent.{Broker, Offer}
import org.zeromq.ZMQ.{Context, Socket}
import com.ergodicity.zeromq.SocketType.ZMQSocketType
import org.slf4j.LoggerFactory
import com.twitter.conversions.time._
import org.zeromq.ZMQ
import annotation.tailrec
import com.twitter.util._

/**
 * Friendly ZMQ queue client
 */
trait Client {
  val log = LoggerFactory.getLogger(classOf[Client])

  type OptionHandler = PartialFunction[Any, Unit]

  def socket: Socket
  
  private val socketOps = new Broker[SocketOption]

  def bind(bind: Bind) {
    socketOps ! bind
  }

  def connect(connect: Connect) {
    socketOps ! connect
  }

  def subscribe(sub: Subscribe) {
    socketOps ! sub
  }

  def unsubscribe(sub: Unsubscribe) {
    socketOps ! sub
  }

  def close() {
    socket.close()
  }

  def send[T](obj: T)(implicit serializer: Serializer[T]) {

    @tailrec def sendFrames(frames: Seq[Frame]) {
      frames match {
        case Nil =>
          socket.send(Array[Byte](), 0)
        case x :: Nil =>
          socket.send(x.payload.toArray, 0)
        case x :: xs =>
          socket.send(x.payload.toArray, ZMQ.SNDMORE)
          sendFrames(xs)
      }
    }

    sendFrames(serializer(obj))
  }
  
  def read[T](implicit deserializer: Deserializer[T], pool: FuturePool): ReadHandle[T]

  def handleConnectionOptions: OptionHandler = {
    case Bind(endpoint) =>
      log.debug("Bind to endpoint: " + endpoint)
      socket.bind(endpoint)

    case Connect(endpoint) =>
      log.debug("Connect to endpoint: " + endpoint)
      socket.connect(endpoint)
  }

  def handleSubscribeOptions: OptionHandler = {
    case Subscribe(payload) =>
      log.debug("Subscribe to: " + payload)
      socket.subscribe(payload.toArray)

    case Unsubscribe(payload) =>
      log.debug("Unsubscrive from: " + payload)
      socket.unsubscribe(payload.toArray)
  }

  def handleUnknownOption: OptionHandler = {
    case opt => log.warn("Skip unknown option: " + opt)
  }
  
  def !(option: SocketOption) {
    socketOps ! option    
  }

  def !(options: Seq[SocketOption]) {
    options foreach {socketOps ! _}
  }

  socketOps.recv foreach {
    handleConnectionOptions orElse handleSubscribeOptions orElse handleUnknownOption
  }
}

object Client {
  val DefaultPollDuration = 1000.millis

  def apply(t: ZMQSocketType, options: Seq[SocketOption] = Seq())(implicit ctx: Context) = {
    val socket = ctx.socket(t.id)
    new ConnectedClient(socket, ctx, options)
  }
}

private[zeromq] sealed trait PollLifeCycle
private[zeromq] case object NoResults extends PollLifeCycle
private[zeromq] case object Results extends PollLifeCycle
private[zeromq] case object Closing extends PollLifeCycle

trait ReadHandle[T] {

  val messages: Offer[T]

  val error: Offer[Throwable]

  def close()
}

object ReadHandle {
  // A convenience constructor using an offer for closing.
  def apply[T](
             _messages: Offer[T],
             _error: Offer[Throwable],
             closeOf: Offer[Unit]
             ): ReadHandle[T] = new ReadHandle[T] {
    val messages = _messages
    val error = _error

    def close() = closeOf()
  }
}


object ClientClosedException extends Exception

protected[zeromq] class ConnectedClient(val socket: Socket, context: Context, options: Seq[SocketOption]) extends Client {self =>
  type Receive = PartialFunction[Any, Unit]

  private sealed abstract class ClientLifecycle
  private case object Poll extends ClientLifecycle
  private case object ReceiveFrames extends ClientLifecycle
  private case class PollError(ex: Throwable) extends ClientLifecycle

  private val noBytes = Array[Byte]()

  self ! options

  private val pollTimeout = {
    val fromConfig = options collectFirst { case PollTimeoutDuration(duration) ⇒ duration }
    fromConfig getOrElse Client.DefaultPollDuration
  }
  
  def read[T](implicit deserializer: Deserializer[T], pool: FuturePool) = {
    val error = new Broker[Throwable]
    val messages = new Broker[T]
    val close = new Broker[Unit]

    val actions = new Broker[ClientLifecycle]

    val poller = context.poller
    poller.register(socket, ZMQ.Poller.POLLIN)

    def newEventLoop: Promise[PollLifeCycle] = {
      (pool {
        val cnt = poller.poll(pollTimeout.inMicroseconds)
        if (cnt > 0 && poller.pollin(0))
          Results
        else
          NoResults
      } onSuccess {
        case Results ⇒ actions ! ReceiveFrames
        case NoResults ⇒ actions ! Poll
      } onFailure {
        case ex ⇒ actions ! PollError(ex)
      }).asInstanceOf[Promise[PollLifeCycle]]
    }

    def receiveFrames(): Seq[Frame] = {
      @tailrec def receiveBytes(next: Array[Byte], currentFrames: Vector[Frame] = Vector.empty): Seq[Frame] = {
        val nwBytes = if (next != null && next.nonEmpty) next else noBytes
        val frames = currentFrames :+ Frame(nwBytes)
        if (socket.hasReceiveMore) receiveBytes(socket.recv(0), frames) else frames
      }

      receiveBytes(socket.recv(0))
    }

    def recv(poll: Option[Promise[PollLifeCycle]]) {
      val currentPoll = poll getOrElse newEventLoop

      Offer.select(
        actions.recv {
          case Poll ⇒ {
            recv(None)
          }
          case ReceiveFrames ⇒ {
            receiveFrames() match {
              case Seq() ⇒
              case frames ⇒ messages ! deserializer(frames)
            }
            recv(None)
          }
          case PollError(ex) ⇒ {
            log.error("There was a problem polling the zeromq socket", ex)
            error ! ex
            recv(None)
          }
        },

        close.recv { _ =>
          currentPoll.cancel()
          error ! ClientClosedException
          poller.unregister(socket)
        }
      )
    }

    // -- Start polling
    recv(None)

    ReadHandle(messages.recv, error.recv, close.send(()))
  }

}