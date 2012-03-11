package com.ergodicity.zeromq

import org.scalatest.Spec
import org.zeromq.ZMQ
import org.slf4j.LoggerFactory
import com.twitter.util.FuturePool
import com.ergodicity.zeromq.SocketType.{Dealer, Sub}
import java.util.concurrent.{TimeUnit, CountDownLatch, Executors}
import com.twitter.conversions.time._
import HeartbeatProtocol._

class HeartbeatNotificationSpec extends Spec {
  val log = LoggerFactory.getLogger(classOf[HeartbeatNotificationSpec])

  describe("Heartbeat Notifications") {


    val PingEndpoint = "inproc://ping-notify-spec"
    val PongEndpoint = "inproc://pong-notify-spec"
    val PingConnection = "inproc://ping-notify-spec"
    val PongConnection = "inproc://pong-notify-spec"

    //    val PingEndpoint = "tcp://*:30000"
    //    val PongEndpoint = "tcp://*:30001"
    //    val PingConnection = "tcp://localhost:30000"
    //    val PongConnection = "tcp://localhost:30001"

    val duration = 350.milliseconds

    implicit val pool = FuturePool(Executors.newSingleThreadExecutor())
    val identifier = Identifier("TestId")


    val ref = HeartbeatRef(PingEndpoint, PongEndpoint)

    it("Should send 'Connected' notification instantly when patient already Alive") {
      implicit val context = ZMQ.context(1)

      val server = new Heartbeat(ref, duration = duration, lossLimit = 10)
      val ping = Client(Sub, options = Connect(PingConnection) :: Subscribe.all :: Nil)
      val pong = Client(Dealer, options = Connect(PongConnection) :: Nil)

      val latch = new CountDownLatch(3)

      val pingHandle = ping.read[Message]
      pingHandle.messages foreach {
        case Ping(u, d) =>
          pong.send[Message](Pong(u, identifier))
          latch.countDown()
        case _ =>
      }

      latch.await(3, TimeUnit.SECONDS)

      var connected = false
      server.track(identifier)() foreach {
        case Connected => connected = true
        case _ =>
      }
      assert(connected)

      // -- Close all
      server.close()
      ping.unsubscribe(Unsubscribe.all)
      pingHandle.close()
      ping.close()
      pong.close();
    }

    it("Should send 'Connected' notification after became alive") {
      implicit val context = ZMQ.context(1)

      val server = new Heartbeat(ref, duration = duration, lossLimit = 10)
      val ping = Client(Sub, options = Connect(PingConnection) :: Subscribe.all :: Nil)
      val pong = Client(Dealer, options = Connect(PongConnection) :: Nil)

      val latch = new CountDownLatch(1)

      var connected = false
      server.track(identifier) foreach {
          case Connected => connected = true; latch.countDown()
          case _ =>
      }

      val pingHandle = ping.read[Message]
      pingHandle.messages foreach {
        case Ping(u, d) => pong.send[Message](Pong(u, identifier))
        case _ =>
      }

      latch.await(3, TimeUnit.SECONDS)
      assert(connected)

      // -- Close all
      server.close()
      ping.unsubscribe(Unsubscribe.all)
      pingHandle.close()
      ping.close()
      pong.close();
    }

    it("Should send 'Lost' notification instantly when patient already Dead") {
      implicit val context = ZMQ.context(1)

      val server = new Heartbeat(ref, duration = duration, lossLimit = 2)
      val ping = Client(Sub, options = Connect(PingConnection) :: Subscribe.all :: Nil)
      val pong = Client(Dealer, options = Connect(PongConnection) :: Nil)

      val latch = new CountDownLatch(1)
      var pongSent = false;

      val pingHandle = ping.read[Message]
      pingHandle.messages foreach {
        case Ping(u, d) =>
          if (!pongSent) {
            pong.send[Message](Pong(u, identifier))
            pongSent = true
          }
          server.getState(identifier) match {
            case Some(Dead) => latch.countDown()
            case _ =>
          }
        case _ =>
      }

      latch.await(3, TimeUnit.SECONDS)

      var lost = false
      server.track(identifier)() foreach {
        case Lost => lost = true
        case _ =>
      }
      assert(lost)

      // -- Close all
      server.close()
      ping.unsubscribe(Unsubscribe.all)
      pingHandle.close()
      ping.close()
      pong.close();
    }

    it("Should send 'Lost' notification when patient Dead") {
      implicit val context = ZMQ.context(1)

      val server = new Heartbeat(ref, duration = duration, lossLimit = 2)
      val ping = Client(Sub, options = Connect(PingConnection) :: Subscribe.all :: Nil)
      val pong = Client(Dealer, options = Connect(PongConnection) :: Nil)

      val latch = new CountDownLatch(1)

      var lost = false
      server.track(identifier) foreach {
        case Lost => lost = true; latch.countDown()
        case _ =>
      }

      var pongSent = false;
      val pingHandle = ping.read[Message]
      pingHandle.messages foreach {
        case Ping(u, d) => if (!pongSent) {
          pong.send[Message](Pong(u, identifier))
          pongSent = true
        }
        case _ =>
      }

      latch.await(3, TimeUnit.SECONDS)
      assert(lost)

      // -- Close all
      server.close()
      ping.unsubscribe(Unsubscribe.all)
      pingHandle.close()
      ping.close()
      pong.close();
    }

    it("Should send only one unique notification for walking deads") {
      implicit val context = ZMQ.context(1)

      val server = new Heartbeat(ref, duration = duration, lossLimit = 2)
      val ping = Client(Sub, options = Connect(PingConnection) :: Subscribe.all :: Nil)
      val pong = Client(Dealer, options = Connect(PongConnection) :: Nil)

      val connectedLatch = new CountDownLatch(1)
      val lostLatch = new CountDownLatch(1)
      val walkingDeadLatch = new CountDownLatch(1)

      var connectedNbr = 0
      var lostNbr = 0
      server.track(identifier) foreach {
        case Connected => connectedNbr += 1; connectedLatch.countDown()
        case Lost => lostNbr += 1; lostLatch.countDown()
      }

      var silence = false
      val pingHandle = ping.read[Message]
      pingHandle.messages foreach {
        case Ping(u, d) =>
          if (!silence) {
            try {
              pong.send[Message](Pong(u, identifier))
            } catch {
              case _ =>
            }
          }
          server.getState(identifier) match {
            case Some(WalkingDead) => walkingDeadLatch.countDown()
            case _ =>
          }
        case _ =>
      }

      connectedLatch.await(3, TimeUnit.SECONDS)
      silence = true
      lostLatch.await(3, TimeUnit.SECONDS)
      silence = false;
      walkingDeadLatch.await(3, TimeUnit.SECONDS)

      assert(server.getState(identifier) match {
        case Some(WalkingDead) => true
        case err => log.error("Error state: "+err); false
      })
      assert(lostNbr == 1)
      assert(connectedNbr == 1)

      // -- Close all
      server.close()
      ping.unsubscribe(Unsubscribe.all)
      pingHandle.close()
      ping.close()
      pong.close();
    }
  }
}
