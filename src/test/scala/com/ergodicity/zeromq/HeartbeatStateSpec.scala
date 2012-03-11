package com.ergodicity.zeromq

import org.slf4j.LoggerFactory
import com.twitter.util.FuturePool
import org.zeromq.ZMQ
import com.ergodicity.zeromq.SocketType.{Sub, Dealer}
import HeartbeatProtocol._
import com.twitter.conversions.time._
import java.util.concurrent.{TimeUnit, CountDownLatch, Executors}
import org.scalatest.{BeforeAndAfter, Spec}

class HeartbeatStateSpec extends Spec with BeforeAndAfter {
  val log = LoggerFactory.getLogger(classOf[HeartbeatStateSpec])

  describe("Heartbeat server state") {

    val PingEndpoint = "inproc://ping-q"
    val PongEndpoint = "inproc://pong-q"
    val PingConnection = "inproc://ping-q"
    val PongConnection = "inproc://pong-q"

    val duration = 250.milliseconds

    implicit val pool = FuturePool(Executors.newSingleThreadExecutor())
    val identifier = Identifier("TestId")

    implicit val context = ZMQ.context(1)
    val ref = HeartbeatRef(PingEndpoint, PongEndpoint)

    it("should return None for no Pong response") {
      val server = new Heartbeat(ref, duration = duration, lossLimit = 10)

      assert(server.getState(identifier) match {
        case None => true
        case _ => false
      })

      server.close()
    }

    it("should return Alive for valid Pong response") {
      val server = new Heartbeat(ref, duration = duration, lossLimit = 10)
      val ping = Client(Sub, options = Connect(PingConnection) :: Subscribe.all :: Nil)
      val pong = Client(Dealer, options = Connect(PongConnection) :: Nil)

      val latch = new CountDownLatch(3)

      val pingHandle = ping.read[Message]
      pingHandle.messages foreach {
          case Ping(uid, d) =>
            log.info("Ping receviced, send Pong")
            pong.send[Message](Pong(uid, identifier))
            latch.countDown()
          case _ => throw new IllegalStateException
      }
      
      var closed = false
      pingHandle.error foreach {
        case ClientClosedException => closed = true        
      }

      // -- Wait for Pong sent and processed
      latch.await(3, TimeUnit.SECONDS)

      // -- Close all
      pingHandle.close()
      server.close()
      ping.close()
      pong.close();

      // -- Verify
      val state = server.getState(identifier)
      log.info("State: "+state)
      assert(state match {
        case Some(Alive(die)) => true
        case _ => false
      })
      assert(closed)
    }

    it("should become Alive -> Dead -> WalkingDead") {
      val server = new Heartbeat(ref, duration = duration, lossLimit = 3)
      val ping = Client(Sub, options = Connect(PingConnection) :: Subscribe.all :: Nil)
      val pong = Client(Dealer, options = Connect(PongConnection) :: Nil)

      val aliveLatch = new CountDownLatch(1)
      val deadLatch = new CountDownLatch(1)
      val walkingDeadLatch = new CountDownLatch(1)

      var pingCnt = 0;

      val pingAlive = ping.read[Message]
      pingAlive.messages foreach {
        case Ping(uid, d) =>
          if (pingCnt == 0) {
            pong.send[Message](Pong(uid, identifier))
          } else if(pingCnt == 1) {
            log.info("Countdown Alive latch")
            aliveLatch.countDown()
          } else if (pingCnt == 3) {
            log.info("Countdown Dead latch")
            deadLatch.countDown()
          }
          if (pingCnt >= 4) {
            pong.send[Message](Pong(uid, identifier))
          }
          if (pingCnt == 5) {
            log.info("Countdown WalkingDead latch")
            walkingDeadLatch.countDown()
          }
          pingCnt += 1
        case _ => throw new IllegalStateException
      }

      // -- Wait for Pong sent and processed
      aliveLatch.await(1, TimeUnit.SECONDS)
      assert(server.getState(identifier) match {
        case Some(Alive(die)) => true
        case state => false
      })

      // -- Wait for death
      deadLatch.await(2, TimeUnit.SECONDS)
      assert(server.getState(identifier) match {
        case Some(Dead) => true
        case _ => false
      })

      // -- Wait for WalkingDead
      walkingDeadLatch.await(3, TimeUnit.SECONDS)
      assert(server.getState(identifier) match {
        case Some(WalkingDead) => true
        case _ => false
      })

      // -- Close all
      pingAlive.close()
      server.close()
      ping.close()
      pong.close();

      // Let all sockets to be closed
      Thread.sleep(duration)
    }

  }

}
