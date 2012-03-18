package com.ergodicity.zeromq

object Frame {
  def apply(text: String): Frame = new Frame(text)
}

case class Frame(payload: Seq[Byte]) {
  def this(bytes: Array[Byte]) = this(bytes.toSeq)
  def this(text: String) = this(text.getBytes("UTF-8"))
}