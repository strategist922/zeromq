package com.ergodicity.zeromq

import org.scalatest.Spec
import org.zeromq.ZMQ
import org.slf4j.LoggerFactory
import com.ergodicity.zeromq.SocketType.{XReq, Sub}
import java.util.concurrent.{TimeUnit, CountDownLatch, Executors}
import com.twitter.conversions.time._
import HeartbeatProtocol._
import java.util.UUID
import com.twitter.util.{Return, FuturePool}

class HeartbeatNotificationSpec extends Spec {
  val log = LoggerFactory.getLogger(classOf[HeartbeatNotificationSpec])

  val PingEndpoint = "inproc://ping"
  val PongEndpoint = "inproc://pong"

  val duration = 500.milliseconds

  implicit val pool = FuturePool(Executors.newCachedThreadPool())

  val identifier = Identifier("TestId")
  val ref = HeartbeatRef(PingEndpoint, PongEndpoint)

  describe("Heartbeat Notifications") {

    it("Should send 'Connected' notification instantly when patient already Alive") {
      implicit val context = ZMQ.context(1)
      val uuid = UUID.randomUUID()

      val server = new Heartbeat(ref, duration = duration, lossLimit = 10)
      val ping = Client(Sub, options = Connect(PingEndpoint) :: Subscribe.all :: Nil)
      val pong = Client(XReq, options = Connect(PongEndpoint) :: Nil)

      val pongReceived = new CountDownLatch(1)

      server.ping(uuid)() foreach {
        case Pong(u, i) if u == uuid && i == identifier => pongReceived.countDown()
      }

      val pingHandle = ping.read[Ping]
      pingHandle.messages foreach {
        case Ping(u) if (u == uuid) => pong.send(Pong(uuid, identifier))
        case _ =>
      }

      assert(pongReceived.await(3, TimeUnit.SECONDS), "Pong not received")

      val notificationLatch = new CountDownLatch(1)
      server.track(identifier)() respond {
        case Return(Connected) => notificationLatch.countDown()
        case _ =>
      }
      assert(notificationLatch.await(3, TimeUnit.SECONDS), "Notification not received")

      // Close all
      pingHandle.close()
      server.stop()
      ping.close()
      pong.close();
      context.term()
    }

    it("Should send 'Connected' notification after became alive") {
      implicit val context = ZMQ.context(1)
      val uuid = UUID.randomUUID()

      val server = new Heartbeat(ref, duration = duration, lossLimit = 10)
      val ping = Client(Sub, options = Connect(PingEndpoint) :: Subscribe.all :: Nil)
      val pong = Client(XReq, options = Connect(PongEndpoint) :: Nil)

      val notificationLatch = new CountDownLatch(1)
      server.track(identifier)() respond {
        case Return(Connected) => notificationLatch.countDown()
        case _ =>
      }

      server.ping(uuid)

      val pingHandle = ping.read[Ping]
      pingHandle.messages foreach {
        case Ping(u) if (u == uuid) => pong.send(Pong(uuid, identifier))
        case _ =>
      }

      assert(notificationLatch.await(3, TimeUnit.SECONDS), "Notification not received")

      // Close all
      pingHandle.close()
      server.stop()
      ping.close()
      pong.close();
      context.term()
    }

    it("Should send 'Lost' notification instantly when patient already Dead") {
      implicit val context = ZMQ.context(1)
      val uuid = UUID.randomUUID()

      val server = new Heartbeat(ref, duration = duration, lossLimit = 1)
      val ping = Client(Sub, options = Connect(PingEndpoint) :: Subscribe.all :: Nil)
      val pong = Client(XReq, options = Connect(PongEndpoint) :: Nil)

      server.ping(uuid)

      val pingHandle = ping.read[Ping]
      pingHandle.messages foreach {
        case Ping(u) if (u == uuid) => pong.send(Pong(uuid, identifier))
        case _ =>
      }

      // Let the Patient to die
      Thread.sleep((duration * 2).inMilliseconds)

      val notificationLatch = new CountDownLatch(1)
      server.track(identifier)() respond {
        case Return(Lost) => notificationLatch.countDown()
        case _ =>
      }
      assert(notificationLatch.await(3, TimeUnit.SECONDS), "Notification not received")

      // Close all
      pingHandle.close()
      server.stop()
      ping.close()
      pong.close();
      context.term()
    }

    it("Should send 'Connected' and 'Lost' notifications") {
      implicit val context = ZMQ.context(1)
      val uuid = UUID.randomUUID()

      val server = new Heartbeat(ref, duration = duration, lossLimit = 1)
      val ping = Client(Sub, options = Connect(PingEndpoint) :: Subscribe.all :: Nil)
      val pong = Client(XReq, options = Connect(PongEndpoint) :: Nil)

      val connectedLatch = new CountDownLatch(1)
      val lostLatch = new CountDownLatch(1)
      server.track(identifier) foreach {
        case Connected => connectedLatch.countDown()
        case Lost => lostLatch.countDown()
        case _ =>
      }

      server.ping(uuid)

      val pingHandle = ping.read[Ping]
      pingHandle.messages foreach {
        case Ping(u) if (u == uuid) => pong.send(Pong(uuid, identifier))
        case _ =>
      }

      assert(connectedLatch.await(2, TimeUnit.SECONDS), "Connected notification not received")
      assert(lostLatch.await(2, TimeUnit.SECONDS), "Loadt notification not received")

      // Close all
      pingHandle.close()
      server.stop()
      ping.close()
      pong.close();
      context.term()
    }

    it("Should send only one unique notification for walking deads") {
      implicit val context = ZMQ.context(1)
      val uuid = UUID.randomUUID()

      val server = new Heartbeat(ref, duration = duration, lossLimit = 1)
      val ping = Client(Sub, options = Connect(PingEndpoint) :: Subscribe.all :: Nil)
      val pong = Client(XReq, options = Connect(PongEndpoint) :: Nil)

      var connectedNbr = 0;
      var lostNbr = 0;
      val connectedLatch = new CountDownLatch(1)
      val lostLatch = new CountDownLatch(1)
      server.track(identifier) foreach {
        case Connected => connectedNbr += 1; connectedLatch.countDown()
        case Lost => lostNbr += 1; lostLatch.countDown()
        case _ =>
      }

      server.ping(uuid)

      val pingHandle = ping.read[Ping]
      pingHandle.messages foreach {
        case Ping(u) if (u == uuid) => pong.send(Pong(uuid, identifier))
        case _ =>
      }

      assert(connectedLatch.await(2, TimeUnit.SECONDS), "Connected notification not received")
      assert(lostLatch.await(2, TimeUnit.SECONDS), "Lost notification not received")

      // Sleep a little bit
      Thread.sleep(duration)
      
      // Raised from death!!!
      val walkingDeadLatch = new CountDownLatch(1)
      server.ping(uuid)() foreach {
        case Pong(u, i) => if (u == uuid && i == identifier) walkingDeadLatch.countDown()
      }

      assert(walkingDeadLatch.await(3, TimeUnit.SECONDS), "Patient not raised from death")

      assert(server.getState(identifier) match {
        case Some(WalkingDead) => true
        case _ => false
      })

      assert(connectedNbr == 1)
      assert(lostNbr == 1)

      // Close all
      pingHandle.close()
      ping.close()
      pong.close();
      server.stop()
      context.term()
    }
  }
}
