package com.ergodicity.zeromq

import org.scalatest.Spec
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.util.concurrent.{TimeUnit, CountDownLatch, Executors}
import com.twitter.conversions.time._
import com.twitter.util.FuturePool
import com.ergodicity.zeromq.SocketType.{Dealer, Pub}
import java.util.UUID
import HeartbeatProtocol._

class PatientSpec extends Spec {
  val log = LoggerFactory.getLogger(classOf[HeartbeatNotificationSpec])

  describe("Patient") {
    val PingEndpoint = "inproc://ping-patient-spec"
    val PongEndpoint = "inproc://pong-patient-spec"

    implicit val pool = FuturePool(Executors.newSingleThreadExecutor())
    val identifier = Identifier("TestId")

    implicit val context = ZMQ.context(1)
    val ref = HeartbeatRef(PingEndpoint, PongEndpoint)

    it("should repond with Pong if it's alive") {
      val ping = Client(Pub, options = Bind(PingEndpoint) :: Nil)
      val pong = Client(Dealer, options = Bind(PongEndpoint) :: Nil)

      val patient = new Patient(ref, identifier)

      val uuid = UUID.randomUUID()
      // -- Send Ping
      ping.send[Message](Ping(uuid))

      // -- Read Pongs
      val latch = new CountDownLatch(1)
      pong.read[Message].messages foreach {
        case pong@Pong(u, i) => {
          log.info("Pong: " + pong)
          if (u == uuid && i == identifier) latch.countDown()
        }
        case _ =>
      }

      if (!latch.await(3, TimeUnit.SECONDS)) {
        throw new IllegalStateException
      }

      patient.close()
      ping.close()
      pong.close()

      Thread.sleep(1000)
    }
  }
}
