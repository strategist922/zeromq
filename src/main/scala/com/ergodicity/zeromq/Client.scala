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
  type OptionHandler = PartialFunction[Any, Unit]

  def socket: Socket
  
  def params: Seq[SocketMeta]

  private val socketOptions = new Broker[SocketOption]

  def bind(bind: Bind) {
    socketOptions ! bind
  }

  def subscribe(sub: Subscribe) {
    socketOptions ! sub
  }

  def unsubscribe(sub: Unsubscribe) {
    socketOptions ! sub
  }

  def read(implicit pool: FuturePool): ReadHandle

  def handleConnectionOptions: OptionHandler = {
    case Bind(endpoint) => socket.bind(endpoint)
  }

  def handleSubscribeOptions: OptionHandler = {
    case Subscribe(payload) => socket.subscribe(payload.toArray)
    case Unsubscribe(payload) => socket.unsubscribe(payload.toArray)
  }

  def handleUnknownOption: OptionHandler = {
    case opt => throw new IllegalArgumentException("Unknown option: " + opt)
  }

  socketOptions.recv foreach {
    handleConnectionOptions orElse handleSubscribeOptions orElse handleUnknownOption
  }
}

object Client {
  val DefaultPollDuration = 100.millis

  def apply(t: ZMQSocketType)(implicit ctx: Context) = {
    val socket = ctx.socket(t.id)
    new ConnectedClient(socket, ctx, Seq())
  }
}

private[zeromq] sealed trait PollLifeCycle
private[zeromq] case object NoResults extends PollLifeCycle
private[zeromq] case object Results extends PollLifeCycle
private[zeromq] case object Closing extends PollLifeCycle

trait ReadHandle {

  val messages: Offer[ZMQMessage]

  val error: Offer[Throwable]

  def close()
}

object ReadHandle {
  // A convenience constructor using an offer for closing.
  def apply(
             _messages: Offer[ZMQMessage],
             _error: Offer[Throwable],
             closeOf: Offer[Unit]
             ): ReadHandle = new ReadHandle {
    val messages = _messages
    val error = _error

    def close() = closeOf()
  }
}


object ClientClosedException extends Exception

protected[zeromq] class ConnectedClient(val socket: Socket, context: Context, val params: Seq[SocketMeta]) extends Client {
  val log = LoggerFactory.getLogger(classOf[ConnectedClient])

  type Receive = PartialFunction[Any, Unit]

  private sealed abstract class ClientLifecycle
  private case object Poll extends ClientLifecycle
  private case object ReceiveFrames extends ClientLifecycle
  private case class PollError(ex: Throwable) extends ClientLifecycle

  private val noBytes = Array[Byte]()

  private val pollTimeout = {
    val fromConfig = params collectFirst { case PollTimeoutDuration(duration) ⇒ duration }
    fromConfig getOrElse Client.DefaultPollDuration
  }

  private val deserializer = new ZMQMessageDeserializer

  def read(implicit pool: FuturePool) = {
    log.debug("Read from socket: " + socket)

    val error = new Broker[Throwable]
    val messages = new Broker[ZMQMessage]
    val close = new Broker[Unit]

    val actions = new Broker[ClientLifecycle]

    val poller = context.poller
    poller.register(socket, ZMQ.Poller.POLLIN)

    def newEventLoop: Promise[PollLifeCycle] = {
      log.debug("New event loop")
      (pool {
        val cnt = poller.poll(1000000/*pollTimeout.inMicroseconds*/)
        log.info("CNT: "+cnt)
        for (i <- 0 to 31) {
          log.info("POLLING #"+i+" = "+poller.pollin(i))
        }
        Thread.sleep(100)
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
            log.debug("Poll")
            recv(None)
          }
          case ReceiveFrames ⇒ {
            log.debug("Receive frames")
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
          poller.unregister(socket)
          currentPoll.cancel()
          error ! ClientClosedException
        }
      )
    }

    // -- Start polling
    recv(None)

    ReadHandle(messages.recv, error.recv, close.send(()))
  }

}