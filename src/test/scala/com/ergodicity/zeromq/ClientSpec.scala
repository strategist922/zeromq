package com.ergodicity.zeromq

import org.scalatest.Spec
import org.mockito.Mockito._
import org.zeromq.ZMQ.{Context, Socket}
import com.ergodicity.zeromq.SocketType.Dealer

class ClientSpec extends Spec {

  describe("ZMQ Client") {
    it("should bind socket") {
      implicit val context = mock(classOf[Context])
      val socket = mock(classOf[Socket])
      when(context.socket(Dealer.id)).thenReturn(socket)
      val client = Client(Dealer)

      client.bind(Bind("endpoint"))

      verify(socket, only()).bind("endpoint")
    }

    it("should subscribe and unsubscribe") {
      implicit val context = mock(classOf[Context])
      val socket = mock(classOf[Socket])
      when(context.socket(Dealer.id)).thenReturn(socket)
      val client = Client(Dealer)

      client.subscribe(Subscribe("subscribe1"))
      client.subscribe(Subscribe("subscribe2"))
      client.unsubscribe(Unsubscribe("subscribe1"))

      verify(socket).subscribe("subscribe1".getBytes)
      verify(socket).subscribe("subscribe2".getBytes)
      verify(socket).unsubscribe("subscribe1".getBytes)
    }
  }

}