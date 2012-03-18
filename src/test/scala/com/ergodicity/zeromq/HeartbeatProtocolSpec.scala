package com.ergodicity.zeromq

import org.scalatest.Spec
import java.util.UUID
import sbinary._
import Operations._

class HeartbeatProtocolSpec extends Spec {
  import HeartbeatProtocol._

  describe("Message protocol") {
    it("should serialize Ping message") {
      val uid = UUID.randomUUID()
      val ping = Ping(uid)

      val bytes = toByteArray(ping)

      val heartbeat = fromByteArray[Ping](bytes)

      assert(heartbeat match {
        case Ping(u) => u == uid
        case _ => false
      })
    }

    it("should serialize Pong message") {
      val uid = UUID.randomUUID()
      val identifier = Identifier("TEST")
      val pong = Pong(uid, identifier)

      val bytes = toByteArray(pong)

      val heartbeat = fromByteArray[Pong](bytes)

      assert(heartbeat match {
        case Pong(u, Identifier("TEST")) => u == uid
        case _ => false
      })
    }

  }

}
