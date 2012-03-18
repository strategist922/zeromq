package com.ergodicity.zeromq

import org.scalatest.Spec
import org.zeromq.ZMQ
import org.slf4j.LoggerFactory
import com.twitter.util.FuturePool
import org.scalatest.matchers.MustMatchers
import SocketType._
import java.util.concurrent.{CountDownLatch, TimeUnit, Executors}
import HeartbeatProtocol._

class ClientReadHandleSpec extends Spec with MustMatchers {
  val log = LoggerFactory.getLogger(classOf[ClientReadHandleSpec])

  val endpoint = "inproc://ClientReadHandleSpec"
  implicit val pool = FuturePool(Executors.newSingleThreadExecutor())

  describe("ZMQ Client ReadHandle") {
    it("should read messages from PUB/SUB socket") {
      implicit val context = ZMQ.context(1)

      val pub = Client(Pub, options = Bind(endpoint) :: Nil)
      val sub = Client(Sub, options = Connect(endpoint) :: Subscribe.all :: Nil)

      val start = System.currentTimeMillis()
      val expectedMessages = 100
      for (i <- 1 to expectedMessages) pub.send(i)

      val readHandle = sub.read[Int]
      val latch = new CountDownLatch(1)

      var readNbr = 0
      readHandle.messages foreach {m =>
        readNbr += 1
        if (readNbr == expectedMessages) latch.countDown()
      }

      @volatile var closed = false
      readHandle.error foreach {
        case ClientClosedException => closed = true
        case _ => assert(false)
      }

      latch.await(5, TimeUnit.SECONDS)
      val end = System.currentTimeMillis()

      log.info("Time: " + (end - start) + " millis")
      pub.close()
      readHandle.close()
      sub.close()

      // -- Assert all messages received and client closed
      log.info("Messages readed: " + readNbr)
      assert(closed)
      assert(readNbr == expectedMessages)
    }
  }

}