package com.ergodicity.zeromq

import org.scalatest.Spec
import org.mockito.Mockito._
import org.zeromq.ZMQ.{Context, Socket}
import com.ergodicity.zeromq.SocketType.XReq
import sbinary.DefaultProtocol._

class ClientSpec extends Spec {

  val Msg = "Ebaka"

  describe("ZMQ Client") {
    it("should bind socket") {
      implicit val context = mock(classOf[Context])
      val socket = mock(classOf[Socket])
      when(context.socket(XReq.id)).thenReturn(socket)
      val client = Client(XReq)

      client.bind(Bind("endpoint"))

      verify(socket, only()).bind("endpoint")
    }

    it("should subscribe and unsubscribe") {
      implicit val context = mock(classOf[Context])
      val socket = mock(classOf[Socket])
      when(context.socket(XReq.id)).thenReturn(socket)
      val client = Client(XReq)

      client.subscribe(Subscribe("subscribe1"))
      client.subscribe(Subscribe("subscribe2"))
      client.unsubscribe(Unsubscribe("subscribe1"))

      verify(socket).subscribe("subscribe1".getBytes)
      verify(socket).subscribe("subscribe2".getBytes)
      verify(socket).unsubscribe("subscribe1".getBytes)
    }

    it("should send message") {
      implicit val context = mock(classOf[Context])
      val socket = mock(classOf[Socket])
      when(context.socket(XReq.id)).thenReturn(socket)
      val client = Client(XReq)
      client.send(Msg)
      import sbinary._
      import Operations._
      verify(socket).send(toByteArray(Msg), 0)
    }
  }
}