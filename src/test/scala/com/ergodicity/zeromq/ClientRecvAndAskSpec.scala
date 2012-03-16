package com.ergodicity.zeromq

import org.scalatest.Spec
import org.slf4j.LoggerFactory
import com.ergodicity.zeromq.SocketType._
import org.zeromq.ZMQ
import Deserializer._
import com.twitter.util.FuturePool
import java.util.concurrent.Executors

class ClientRecvAndAskSpec extends Spec {
  val log = LoggerFactory.getLogger(classOf[ClientRecvAndAskSpec])

  val Endpoint = "inproc://recv-ask-spec"

  describe("Client recv&ask") {
    it("should recevice sent message") {
      implicit val context = ZMQ.context(1)

      val message = "Message"

      val server = Client(Rep, options = Bind(Endpoint) :: Nil)
      val client = Client(Req, options = Connect(Endpoint) :: Nil)
      
      client.send(message)
      
      val received = server.recv[String]
      
      log.info("Received: "+received)
      assert(received == message)
      
      server.close()
      client.close()
      context.term()
    }

    it("should return answer for ask") {
      implicit val context = ZMQ.context(1)
      implicit val pool = FuturePool(Executors.newCachedThreadPool)

      val message = "Hello"

      val server = Client(Rep, options = Bind(Endpoint) :: Nil)
      val handle = server.read[String]
      handle.messages foreach {
        case "Hello" => server.send("Hello World")
        case _ =>
      }
      val client = Client(Req, options = Connect(Endpoint) :: Nil)

      val reply = client ? message
      log.info("Reply: " + reply)
      assert(reply == "Hello World")

      handle.close()
      server.close()
      client.close()
      context.term()
    }
  }
}