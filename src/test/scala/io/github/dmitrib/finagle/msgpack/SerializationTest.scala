package io.github.dmitrib.finagle.msgpack

import org.junit.Test
import org.msgpack.MessagePack
import org.junit.Assert._

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
class SerializationTest {
  val msgpack = new MessagePack

  @Test def rpcRequestSerialization() {
    val obj = new RpcRequest("op", "test", Seq(new Integer(1)))
    val bytes = msgpack.write(obj)
    val res = msgpack.read(bytes, classOf[RpcRequest])
    assertEquals(obj, res)
  }

  @Test def rpcRequestNullArgSerialization() {
    val obj = new RpcRequest("op", "test", Seq(null))
    val bytes = msgpack.write(obj)
    val res = msgpack.read(bytes, classOf[RpcRequest])
    assertEquals(obj, res)
  }

  @Test def rpcResponseSerialization() {
    val obj = new RpcResponse(new Integer(1), false)
    val bytes = msgpack.write(obj)
    val res = msgpack.read(bytes, classOf[RpcResponse])
    assertEquals(obj, res)
  }

  @Test def nullRpcResponseSerialization() {
    val obj = new RpcResponse(null, false)
    val bytes = msgpack.write(obj)
    val res = msgpack.read(bytes, classOf[RpcResponse])
    assertEquals(obj, res)
  }

  @Test def exceptionSerialization() {
    val obj = try {
      throw new RuntimeException("test")
    } catch {
      case e: Exception => {
        new SerializableTransportWrapper(e)
      }
    }
    val bytes = msgpack.write(obj)
    val res = msgpack.read(bytes, classOf[SerializableTransportWrapper])
    val initialEx = obj.obj.asInstanceOf[RuntimeException]
    val actualEx = res.obj.asInstanceOf[RuntimeException]
    assertEquals(initialEx.getMessage, actualEx.getMessage)
    assertEquals(initialEx.getStackTraceString, actualEx.getStackTraceString)
  }
}
