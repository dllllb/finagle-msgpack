package io.github.dmirib.finagle.msgpack

import scala.collection.mutable

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
class RpcServer(workerThreads: Int) {
  val handlers = mutable.HashMap.empty[String, AnyRef]

  def registerHandler(name: String, handler: AnyRef) {}
}
