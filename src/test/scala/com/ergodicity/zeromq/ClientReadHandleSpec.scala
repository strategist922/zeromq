package com.ergodicity.zeromq

import org.scalatest.Spec
import org.zeromq.ZMQ
import org.slf4j.LoggerFactory
import com.twitter.util.FuturePool
import com.twitter.conversions.time._
import org.scalatest.matchers.MustMatchers
import SocketType._
import java.util.concurrent.{CountDownLatch, TimeUnit, Executors}

class ClientReadHandleSpec extends Spec with MustMatchers {
  val log = LoggerFactory.getLogger(classOf[ClientReadHandleSpec])

  describe("ZMQ Client ReadHandle") {
    val endpoint = "tcp://*:12345"
    val connect = "tcp://localhost:12345"
    implicit val pool = FuturePool(Executors.newSingleThreadExecutor())

    it("should read messages from PUB/SUB socket") {
      implicit val context = ZMQ.context(1)

      val pub = Client(Pub, options = Bind(endpoint) :: Nil)
      val client = Client(Sub, options = Connect(connect) :: Subscribe.all :: PollTimeoutDuration(100.milliseconds) :: Nil)

      val expectedMessages = 1000
      for (i <- 1 to expectedMessages) pub.send("Message#"+i)

      val readHandle = client.read[ZMQMessage]

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
      readHandle.close()
      pub.close()

      // -- Assert all messages received and client closed
      log.info("Messages readed: " + readNbr)
      assert(closed)
      assert(readNbr == expectedMessages)
    }
  }

}