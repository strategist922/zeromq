package com.ergodicity.zeromq

import org.scalatest.Spec
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.util.concurrent.{TimeUnit, CountDownLatch, Executors}
import com.twitter.util.FuturePool
import com.ergodicity.zeromq.SocketType._
import java.util.UUID
import HeartbeatProtocol._

class PatientSpec extends Spec {
  val log = LoggerFactory.getLogger(classOf[HeartbeatNotificationSpec])

  describe("Patient") {
    val PingEndpoint = "inproc://ping"
    val PongEndpoint = "inproc://pong"

    implicit val pool = FuturePool(Executors.newSingleThreadExecutor())
    val identifier = Identifier("TestId")

    implicit val context = ZMQ.context(1)
    val ref = HeartbeatRef(PingEndpoint, PongEndpoint)

    it("should repond with Pong if it's alive") {
      val ping = Client(Pub, options = Bind(PingEndpoint) :: Nil)
      val pong = Client(XReq, options = Bind(PongEndpoint) :: Nil)

      val patient = new Patient(ref, identifier)

      val uuid = UUID.randomUUID()
      // Send Ping
      ping.send(Ping(uuid))

      // Read Pongs
      val latch = new CountDownLatch(1)
      val handle = pong.read[Pong]
      handle.messages foreach {
        case Pong(u, i) if (u == uuid && i == identifier) => latch.countDown()
        case _ =>
      }

      if (!latch.await(3, TimeUnit.SECONDS)) {
        throw new IllegalStateException
      }

      // Close all
      handle.close()
      patient.close()
      ping.close()
      pong.close()

      Thread.sleep(1000)
    }
  }
}
