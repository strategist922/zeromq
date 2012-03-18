package com.ergodicity.zeromq

import java.util.UUID
import org.slf4j.LoggerFactory
import com.twitter.finagle.util.Timer
import com.twitter.conversions.time._
import org.jboss.netty.util.HashedWheelTimer
import sbinary._
import Operations._
import com.twitter.util.{TimerTask, FuturePool, Duration}
import concurrent.stm._
import com.ergodicity.zeromq.SocketType._
import com.twitter.concurrent.{Offer, Broker}
import org.zeromq.ZMQ.Context

case class Identifier(id: String)

protected case class Ping(uid: UUID)
protected case class Pong(uid: UUID, identifier: Identifier)

object HeartbeatProtocol extends DefaultProtocol {

  implicit object DurationFormat extends Format[Duration] {
    def reads(in: Input) = read[Long](in).milliseconds

    def writes(out: Output, duration: Duration) {
      write[Long](out, duration.inMilliseconds)
    }
  }

  implicit object IdentifierFormat extends Format[Identifier] {
    def reads(in: Input) = Identifier(read[String](in))

    def writes(out: Output, identifier: Identifier) {
      write[String](out, identifier.id)
    }
  }

  implicit object UUIDFormat extends Format[UUID] {
    def reads(in: Input) = UUID.fromString(read[String](in))

    def writes(out: Output, uuid: UUID) {
      write[String](out, uuid.toString)
    }
  }

  implicit object PingFormat extends Format[Ping] {
    def reads(in: Input) = Ping(read[UUID](in))

    def writes(out: Output, value: Ping) {
      write[UUID](out, value.uid)
    }
  }

  implicit object PongFormat extends Format[Pong] {
    def reads(in: Input) = Pong(read[UUID](in), read[Identifier](in))

    def writes(out: Output, value: Pong) {
      write[UUID](out, value.uid)
      write[Identifier](out, value.identifier)
    }
  }
}


case class HeartbeatRef(ping: String, pong: String)


protected sealed trait State
protected case class Alive(die: TimerTask) extends State
protected case object Dead extends State
protected case object WalkingDead extends State


sealed trait Notification
case object Connected extends Notification
case object Lost extends Notification


class Heartbeat(ref: HeartbeatRef, duration: Duration = 1.second, lossLimit: Int = 3)
               (implicit context: Context, pool: FuturePool) {

  import HeartbeatProtocol._

  private val log = LoggerFactory.getLogger(classOf[Heartbeat])

  private val Timer = new Timer(new HashedWheelTimer())

  private val pendingUUID = Ref(List[(UUID, Broker[Pong])]())
  private val patients = Ref(Map[Identifier, State]())
  private val trackers = Ref(List[(Identifier, Broker[Notification])]())

  private val ping = Client(Pub, options = Bind(ref.ping) :: Nil)
  private val pong = Client(XReq, options = Bind(ref.pong) :: Nil)

  def ping(uuid: UUID): Offer[Pong] = {
    val broker = new Broker[Pong]()
    atomic {implicit txn =>
        pendingUUID.transform(l => ((uuid, broker) :: l).slice(0, lossLimit))
    }
    ping.send(Ping(uuid))
    broker.recv
  }

  def start = Timer.schedule(duration) {
    ping(UUID.randomUUID())
  }

  // -- Handle Pong messages
  val pongHandle = pong.read[Pong]
  pongHandle.messages foreach {
    msg =>
      val pong = msg.payload
      val uid = pong.uid
      val identifier = pong.identifier

      pendingUUID.single() find (_._1 == uid) foreach {
        tuple => // only if UUID presented
          val notification = atomic {
            implicit txt =>
              val (state, notification) = patients().get(identifier).map {
                case Alive(die) => die.cancel(); (Alive(scheduleDeath(identifier)), None)
                case Dead => (WalkingDead, None)
                case WalkingDead => (WalkingDead, None)
              } getOrElse {
                (Alive(scheduleDeath(identifier)), Some(Connected))
              }
              patients.transform(_ + (identifier -> state))
              notification
          }

          // Notify trackers
          notification foreach {
            notify =>
              trackers.single() filter (_._1 == identifier) foreach {
                _._2 ! notify
              }
          }

          // Forward Pong to broker
          tuple._2 ! pong
      }
      msg.ack()
  }

  pongHandle.error foreach {
    case ClientClosedException => log.debug("Pong handle closed")
    case err => log.error("Heartbeat server error: " + err)
  }

  private def scheduleDeath(identifier: Identifier) = {
    Timer.schedule((duration * lossLimit).fromNow) {
      log.info("Patient died: " + identifier)
      trackers.single() filter (_._1 == identifier) foreach {
        _._2 ! Lost
      }
      atomic {
        implicit txn =>
          patients.transform(_ + (identifier -> Dead))
          trackers.transform(_.filterNot(_._1 == identifier))
      }
    }
  }

  def getState(identifier: Identifier) = patients.single() get(identifier)

  def track(identifier: Identifier) = {
    log.info("Track events for: " + identifier)
    val broker = new Broker[Notification]

    // If we already has state for given identifier send event
    val instantNotification: Option[Notification] = atomic {
      implicit txn =>
        def addTracker(implicit txt: InTxn) = trackers.transform((identifier, broker) :: _)

        patients().get(identifier) map {
          case Alive(die) => addTracker; Some(Connected)
          case Dead => Some(Lost)
          case WalkingDead => Some(Lost)
        } getOrElse {
          addTracker
          None
        }
    }
    instantNotification foreach (broker ! _)
    broker.recv
  }

  def stop() {
    log.info("Stop Heartbeat")
    atomic {implicit txn =>
      pendingUUID.transform(_ => List())
      trackers.transform(_ => List())
    }
    pongHandle.close()
    pong.close()
    ping.close()
    Timer.stop()
  }
}


class Patient(ref: HeartbeatRef, identifier: Identifier)
             (implicit context: Context, pool: FuturePool) {
  import HeartbeatProtocol._

  val ping = Client(Sub, options = Connect(ref.ping) :: Subscribe.all :: Nil)
  val pong = Client(XReq, options = Connect(ref.pong) :: Nil)

  val pingHandle = ping.read[Ping]
  pingHandle.messages foreach {
    msg =>
      val ping = msg.payload
      pong.send(Pong(ping.uid, identifier))
      msg.ack()
  }

  def close() {
    pingHandle.close()
    ping.unsubscribe(Unsubscribe.all)
    ping.close()
    pong.close()
  }
}


