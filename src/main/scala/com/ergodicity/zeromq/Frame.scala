package com.ergodicity.zeromq

object Frame {
  def apply(text: String): Frame = new Frame(text)
}

/**
 * A single message frame of a zeromq message
 * @param payload
 */
case class Frame(payload: Seq[Byte]) {
  def this(bytes: Array[Byte]) = this(bytes.toSeq)
  def this(text: String) = this(text.getBytes("UTF-8"))
}

/**
 * A message received over the zeromq socket
 * @param frames
 */
case class ZMQMessage(frames: Seq[Frame]) {

  def this(frame: Frame) = this(Seq(frame))
  def this(frame1: Frame, frame2: Frame) = this(Seq(frame1, frame2))
  def this(frameArray: Array[Frame]) = this(frameArray.toSeq)

  /**
   * Convert the bytes in the first frame to a String, using specified charset.
   */
  def firstFrameAsString(charsetName: String): String = new String(frames.head.payload.toArray, charsetName)
  /**
   * Convert the bytes in the first frame to a String, using "UTF-8" charset.
   */
  def firstFrameAsString: String = firstFrameAsString("UTF-8")

  def payload(frameIndex: Int): Array[Byte] = frames(frameIndex).payload.toArray
}

object ZMQMessage {
  def apply(bytes: Array[Byte]): ZMQMessage = ZMQMessage(Seq(Frame(bytes)))
}

trait Deserializer[T] {
  def apply(frames: Seq[Frame]): T
}

object Deserializer {
  implicit val ZMQMessageDeserializer = new Deserializer[ZMQMessage] {
    def apply(frames: Seq[Frame]) = ZMQMessage(frames)
  }
}

trait Serializer[T] {
  def apply(obj: T): Seq[Frame]
}


object Serializer {
  implicit val StringSerializer = new Serializer[String] {
    def apply(obj: String) = Seq(Frame(obj))
  }
}
