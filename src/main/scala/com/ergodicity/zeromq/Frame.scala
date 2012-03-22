package com.ergodicity.zeromq

import sbinary._
import sbinary.Operations._

object Frame {
  def apply(text: String): Frame = new Frame(text)
}

case class Frame(payload: Seq[Byte]) {
  def this(bytes: Array[Byte]) = this(bytes.toSeq)
  def this(text: String) = this(text.getBytes("UTF-8"))
}


trait Serializer[E] {
  def apply(obj: E): Seq[Frame]
}

trait Deserializer[E] {
  def apply(frames: Seq[Frame]): E
}

object Serializer {
  implicit def writes2serializer[E](implicit writes: Writes[E]) = new Serializer[E] {
    def apply(obj: E) = Seq(Frame(toByteArray(obj)))
  }
}

object Deserializer {
  implicit def reads2deserializer[E](implicit reads: Reads[E]) = new Deserializer[E] {
    def apply(frames: Seq[Frame]) = frames match {
      case Seq(frame) => fromByteArray[E](frame.payload.toArray)
      case err => throw new IllegalArgumentException("Illegal frames sequence: " + err)
    }
  } 
}