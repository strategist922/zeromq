package com.ergodicity.zeromq

import org.scalatest.Spec
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import com.ergodicity.zeromq.SocketType.{Dealer, Sub}
import java.util.concurrent.{TimeUnit, CountDownLatch, Executors}
import com.twitter.conversions.time._
import com.twitter.util.FuturePool
import collection.immutable.Stack

class PatientSpec extends Spec {
  val log = LoggerFactory.getLogger(classOf[HeartbeatNotificationSpec])

  describe("Patient") {
    val PingEndpoint = "inproc://ping-patient-spec"
    val PongEndpoint = "inproc://pong-patient-spec"

    val duration = 250.milliseconds

    implicit val pool = FuturePool(Executors.newSingleThreadExecutor())
    val identifier = Identifier("TestId")

    implicit val context = ZMQ.context(1)
    val ref = HeartbeatRef(PingEndpoint, PongEndpoint)

    it("should mark itself as Alive") {
      val server = new Heartbeat(ref, duration = duration, lossLimit = 10)
      val patient = new Patient(ref, identifier)

      val latch = new CountDownLatch(1)
      var notifications = List[Notification]()
      server.track(identifier) foreach {n =>
        log.info("Notification: "+n)
        notifications = n :: notifications
        latch.countDown()
      }

      latch.await(3, TimeUnit.SECONDS)

      assert(notifications.size == 1)
      assert(notifications.head == Connected)
      assert(server.getState(identifier) match {
        case Some(Alive(death)) => true
        case _ => false
      })
            
      server.close()
      patient.close()
    }
  }
}
