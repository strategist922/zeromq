package com.ergodicity.zeromq

import org.slf4j.LoggerFactory
import com.twitter.util.FuturePool
import org.zeromq.ZMQ
import HeartbeatProtocol._
import com.twitter.conversions.time._
import java.util.concurrent.{TimeUnit, CountDownLatch, Executors}
import org.scalatest.{BeforeAndAfter, Spec}
import java.util.UUID
import com.ergodicity.zeromq.SocketType.{Sub, XReq}

class HeartbeatStateSpec extends Spec with BeforeAndAfter {
  val log = LoggerFactory.getLogger(classOf[HeartbeatStateSpec])

  describe("Heartbeat server state") {

    val PingEndpoint = "inproc://ping"
    val PongEndpoint = "inproc://pong"

    val duration = 250.milliseconds

    implicit val pool = FuturePool(Executors.newCachedThreadPool())
    val identifier = Identifier("TestId")

    implicit val context = ZMQ.context(1)
    val ref = HeartbeatRef(PingEndpoint, PongEndpoint)

    it("should return None for no Pong response") {
      val server = new Heartbeat(ref, duration = duration, lossLimit = 10)

      assert(server.getState(identifier) match {
        case None => true
        case _ => false
      })

      server.stop()
    }

    it("should send Ping requests with given UUID") {
      val uuid = UUID.randomUUID()

      val server = new Heartbeat(ref, duration = duration, lossLimit = 10)
      val ping = Client(Sub, options = Connect(PingEndpoint) :: Subscribe.all :: Nil)

      server.ping(uuid)

      val pingReceived = new CountDownLatch(1)
      val pingHandle = ping.read[Message]
      pingHandle.messages() foreach {
        case Ping(u) => if (u == uuid) pingReceived.countDown();
        case _ =>
      }

      assert(pingReceived.await(3, TimeUnit.SECONDS), "Ping not received")

      pingHandle.close()
      ping.close()
      server.stop()
    }

    it("should return Alive for valid Pong response") {
      val uuid = UUID.randomUUID()

      val server = new Heartbeat(ref, duration = duration, lossLimit = 10)
      val ping = Client(Sub, options = Connect(PingEndpoint) :: Subscribe.all :: Nil)
      val pong = Client(XReq, options = Connect(PongEndpoint) :: Nil)

      val pongReceived = new CountDownLatch(1)

      server.ping(uuid)() foreach {pong: Pong =>
        log.debug("Got pong: "+pong)
        pongReceived.countDown()
      }

      val pingHandle = ping.read[Message]
      pingHandle.messages foreach {
        case Ping(u) => if (u == uuid) pong.send[Message](Pong(uuid, identifier))
        case _ =>
      }

      assert(pongReceived.await(3, TimeUnit.SECONDS), "Pong not received")
      
      // -- Verify
      val state = server.getState(identifier)
      log.info("State: "+state)
      assert(state match {
        case Some(Alive(die)) => true
        case _ => false
      })

      // -- Close all
      pingHandle.close()
      server.stop()
      ping.close()
      pong.close();
    }

    it("should become Alive -> Dead -> WalkingDead") {
      val uuid = UUID.randomUUID()

      val server = new Heartbeat(ref, duration = duration, lossLimit = 1)
      val ping = Client(Sub, options = Connect(PingEndpoint) :: Subscribe.all :: Nil)
      val pong = Client(XReq, options = Connect(PongEndpoint) :: Nil)

      val pongReceived = new CountDownLatch(1)
      server.ping(uuid)() foreach {pong: Pong =>
        log.debug("Got pong: "+pong)
        pongReceived.countDown()
      }

      val pingHandle = ping.read[Message]
      pingHandle.messages foreach {
        case Ping(u) => if (u == uuid) pong.send[Message](Pong(uuid, identifier))
        case _ =>
      }

      assert(pongReceived.await(3, TimeUnit.SECONDS), "Pong not received")

      // Verify Alive
      assert(server.getState(identifier) match {
        case Some(Alive(die)) => true
        case _ => false
      })
      
      // Sleap to let patient to die
      Thread.sleep((duration * 2).inMillis)

      // Verify dead
      assert(server.getState(identifier) match {
        case Some(Dead) => true
        case _ => false
      })

      // Raised from death!!!
      val deadPongReceived = new CountDownLatch(1)
      server.ping(uuid)() foreach {pong: Pong =>
        log.debug("Got pong: "+pong)
        deadPongReceived.countDown()
      }

      assert(deadPongReceived.await(3, TimeUnit.SECONDS), "Pong not received")

      // Verify WalkingDead
      assert(server.getState(identifier) match {
        case Some(WalkingDead) => true
        case _ => false
      })

      // Close all
      pingHandle.close()
      server.stop()
      ping.close()
      pong.close();
    }

  }

}
