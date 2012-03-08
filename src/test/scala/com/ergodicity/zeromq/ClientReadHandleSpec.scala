package com.ergodicity.zeromq

import org.scalatest.Spec
import org.zeromq.ZMQ
import org.slf4j.LoggerFactory
import com.twitter.util.FuturePool
import org.scalatest.matchers.MustMatchers
import org.zeromq.ZMQ.Context
import java.util.Arrays
import SocketType._
import java.util.concurrent.{TimeUnit, Executors}

class ClientReadHandleSpec extends Spec with MustMatchers {
  val log = LoggerFactory.getLogger(classOf[ClientReadHandleSpec])

  def connectSubscriber(context: Context) = {
    val socket = context.socket(ZMQ.SUB)
    socket.connect("inproc://zmq-spec")
    socket.subscribe(Array.empty)
    socket
  }
  lazy val outgoingMessage = "hello"
  
  describe("ZMQ Client ReadHandle") {
    val endpoint = "inproc://client-spec"
    implicit val pool = FuturePool(Executors.newSingleThreadExecutor())

    it("should read messages from socket") {
      System.load("C:\\Program Files (x86)\\ZeroMQ 2.1.10\\lib\\libzmq.dll")
/*      val context = ZMQ.context(1)
      val (pub, sub, poller) = (
        context.socket(ZMQ.PUB),
        context.socket(ZMQ.SUB),
        context.poller
        )
      pub.bind("inproc://zmq-spec")
      sub.connect("inproc://zmq-spec")
      sub.subscribe(Array.empty)
      poller.register(sub)
      pub.send(outgoingMessage.getBytes, 0)
      poller.poll must equal(1)
      poller.pollin(0) must equal(true)
      val incomingMessage = sub.recv(0)
      incomingMessage must equal(outgoingMessage.getBytes)
      sub.close
      pub.close*/

      implicit val context = ZMQ.context(1)

      val client = Client(Dealer)
      client.bind(Bind(endpoint))
      client.subscribe(Subscribe.all)

      val socket = context.socket(ZMQ.REQ)
      socket.connect(endpoint)

      val readHandle = client.read
      readHandle.messages() foreach {m =>
        log.info("RECEIVED MESSAGE: " + m)
      }

      readHandle.error() foreach {err =>
        log.error("ERROR: "+err)
      }

      Thread.sleep(100)

      val mess = "Message".getBytes
      log.info("Send message: " + Arrays.toString(mess))
      socket.send(mess, 0)
      log.info("Message sent: " + Arrays.toString(mess))

      Thread.sleep(TimeUnit.SECONDS.toMillis(10))

      readHandle.close()

      Thread.sleep(100)
    }
  }

}