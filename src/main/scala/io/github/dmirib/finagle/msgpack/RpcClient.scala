package io.github.dmirib.finagle.msgpack

import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import java.net.SocketAddress

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
class RpcClient(hosts: Seq[SocketAddress], connectionLimit: Int = 1) {
  val client: Service[MsgPackRequest, MsgPackResponse] = ClientBuilder()
    .codec(new MsgPackCodec())
    .hosts(hosts)
    .hostConnectionLimit(connectionLimit)
    .build()

  def service[T](name: String, interface: Class[T]): T = {
    null
  }

  def close = {
    client.close()
  }
}
