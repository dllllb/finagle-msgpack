package io.github.dmirib.finagle.msgpack

import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import java.net.SocketAddress
import java.lang.reflect.{Method, InvocationHandler}

class ClientProxy extends InvocationHandler {
  def invoke(proxy: scala.Any, method: Method, args: Array[AnyRef]): AnyRef = {
    ???
  }
}

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
class RpcClient(hosts: Seq[SocketAddress], connectionLimit: Int = 1) {
  val client: Service[MsgPackRequest, MsgPackResponse] = ClientBuilder()
    .codec(new MsgPackCodec())
    .hosts(hosts)
    .hostConnectionLimit(connectionLimit)
    .build()

  def service[T](id: String, interface: Class[T]): T = {
    val proxy = new ClientProxy
    java.lang.reflect.Proxy.newProxyInstance(interface.getClassLoader, Array(interface), proxy).asInstanceOf[T]
  }

  def close = {
    client.close()
  }
}
