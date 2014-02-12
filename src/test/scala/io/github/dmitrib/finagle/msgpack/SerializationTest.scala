package io.github.dmitrib.finagle.msgpack

import org.junit.Test
import org.msgpack.MessagePack
import org.junit.Assert._

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
class SerializationTest {
  @Test def rpcRequestSerialization() {
    val msgpack = new MessagePack
    val obj = new RpcRequest("op", "test", Seq(new Integer(1)))
    val bytes = msgpack.write(obj)
    val res = msgpack.read(bytes, classOf[RpcRequest])
    assertEquals(obj, res)
  }

  @Test def rpcRequestNullArgSerialization() {
    val msgpack = new MessagePack
    val obj = new RpcRequest("op", "test", Seq(null))
    val bytes = msgpack.write(obj)
    val res = msgpack.read(bytes, classOf[RpcRequest])
    assertEquals(obj, res)
  }

  @Test def rpcResponseSerialization() {
    val msgpack = new MessagePack
    val obj = new RpcResponse(new Integer(1), false)
    val bytes = msgpack.write(obj)
    val res = msgpack.read(bytes, classOf[RpcResponse])
    assertEquals(obj, res)
  }

  @Test def nullRpcResponseSerialization() {
    val msgpack = new MessagePack
    val obj = new RpcResponse(null, false)
    val bytes = msgpack.write(obj)
    val res = msgpack.read(bytes, classOf[RpcResponse])
    assertEquals(obj, res)
  }
}
