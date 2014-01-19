package io.github.dmitrib.finagle.msgpack

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
class RpcException(message: String, reason: Throwable) extends RuntimeException(message, reason) {
  def this(message: String) = this(message, null)
}
