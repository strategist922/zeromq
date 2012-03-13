package com.ergodicity.zeromq

import org.scalatest.Spec
import org.mockito.Mockito._
import org.zeromq.ZMQ.{Context, Socket}
import com.ergodicity.zeromq.SocketType.XReq
import org.zeromq.ZMQ

class ClientSpec extends Spec {

  case class Msg()

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

    it("should send empty frames") {
      implicit val context = mock(classOf[Context])
      val socket = mock(classOf[Socket])
      when(context.socket(XReq.id)).thenReturn(socket)
      val client = Client(XReq)

      implicit val zeroFramesSerializer = new Serializer[Msg] {
        def apply(obj: ClientSpec.this.type#Msg) = Seq()
      }
      client.send(Msg())

      verify(socket).send(Array[Byte](), 0)
    }

    it("should send one frame") {
      implicit val context = mock(classOf[Context])
      val socket = mock(classOf[Socket])
      when(context.socket(XReq.id)).thenReturn(socket)
      val client = Client(XReq)

      val bytes = "Bytes".getBytes
      implicit val zeroFramesSerializer = new Serializer[Msg] {
        def apply(obj: ClientSpec.this.type#Msg) = Seq(Frame(bytes))
      }
      client.send(Msg())

      verify(socket).send(bytes, 0)
    }

    it("should send two and more frame") {
      implicit val context = mock(classOf[Context])
      val socket = mock(classOf[Socket])
      when(context.socket(XReq.id)).thenReturn(socket)
      val client = Client(XReq)

      val bytes1 = "Bytes1".getBytes
      val bytes2 = "Bytes2".getBytes
      val bytes3 = "Bytes3".getBytes
      implicit val zeroFramesSerializer = new Serializer[Msg] {
        def apply(obj: ClientSpec.this.type#Msg) = Seq(Frame(bytes1), Frame(bytes2), Frame(bytes3))
      }
      client.send(Msg())

      verify(socket).send(bytes1, ZMQ.SNDMORE)
      verify(socket).send(bytes2, ZMQ.SNDMORE)
      verify(socket).send(bytes3, 0)
    }

  }

}