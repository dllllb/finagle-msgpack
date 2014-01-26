package io.github.dmitrib.finagle.msgpack

import com.twitter.finagle.{Codec, CodecFactory}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.msgpack.{MessagePackable, MessagePack}
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder
import org.msgpack.annotation.Message
import org.msgpack.unpacker.Unpacker
import org.msgpack.packer.Packer

@Message
case class ExceptionTransportWrapper(var exception: Exception) extends MessagePackable {
  //for msgpack serialization
  def this() = this(null)

  def readFrom(u: Unpacker) {
    val exClassStr = u.readString()
    val msg = u.readString()

    val exClass = try {
      Class.forName(exClassStr)
    } catch {
      case e: Exception => throw {
        new RpcException("can't deserialize message", e)
      }
    }

    exception = (if (msg != null) {
      exClass.getConstructor(classOf[String]).newInstance(msg)
    } else {
      exClass.newInstance()
    }).asInstanceOf[Exception]
  }

  def writeTo(pk: Packer) {
    pk.write(exception.getClass.toString)
    pk.write(exception.getMessage)
  }
}

@Message
case class RpcRequest(var method: String,
                      var serviceId: String,
                      var args: Seq[AnyRef],
                      var paramTypes: Seq[Class[_]]) extends MessagePackable {
  //for msgpack serialization
  def this() = this(null, null, null, null)

  def callId = s"$serviceId:$method"

  def readFrom(u: Unpacker) {
    method = u.readString()
    serviceId = u.readString()

    val paramLength = u.readArrayBegin()
    paramTypes = (0 until paramLength) map { i =>
      val typeName = u.readString()
      try {
        Class.forName(typeName)
      } catch {
        case e: Exception => throw {
          new RpcException("can't deserialize message", e)
        }
      }
    }
    u.readArrayEnd()

    u.readArrayBegin()
    args = paramTypes map { klass =>
      val value = u.read(klass.asInstanceOf[Class[AnyRef]])
      value
    }
    u.readArrayEnd()
  }

  def writeTo(pk: Packer) {
    pk.write(method)
    pk.write(serviceId)

    pk.writeArrayBegin(paramTypes.size)
    paramTypes.map(_.getName).foreach(pk.write)
    pk.writeArrayEnd()

    pk.writeArrayBegin(args.size)
    args.foreach(pk.write)
    pk.writeArrayEnd()
  }
}

@Message
case class RpcResponse(var response: AnyRef, var failed: Boolean) extends MessagePackable {
  //for msgpack serialization
  def this() = this(null, false)

  def readFrom(u: Unpacker) {
    val typeName = u.readString()
    val klass = try {
      Class.forName(typeName)
    } catch {
      case e: Exception => throw {
        new RpcException("can't deserialize message", e)
      }
    }
    response = u.read(klass.asInstanceOf[Class[AnyRef]])
    failed = u.readBoolean()
  }

  def writeTo(pk: Packer) {
    pk.write(response.getClass.getName)
    pk.write(response)
    pk.write(failed)
  }
}

class MsgPackDecoder(val msgClass: Class[_]) extends LengthFieldBasedFrameDecoder(Int.MaxValue, 0, 4, 0, 4) {
  val msgpack = new MessagePack


  override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): AnyRef = {
    val buf = super.decode(ctx, channel, buffer).asInstanceOf[ChannelBuffer]

    if (buf == null) {
      return null
    }

    val msgBuf = new Array[Byte](buf.readableBytes())
    buf.readBytes(msgBuf)
    val value = msgpack.read(msgBuf, msgClass)
    value.asInstanceOf[AnyRef]
  }
}

class MsgPackEncoder extends OneToOneEncoder {
  val msgpack = new MessagePack

  def encode(ctx: ChannelHandlerContext, channel: Channel, msg: scala.Any): AnyRef = {
    val bytes = msgpack.write(msg)

    val msgLenBuf = ChannelBuffers.buffer(4)
    msgLenBuf.writeInt(bytes.size)

    ChannelBuffers.wrappedBuffer(msgLenBuf, ChannelBuffers.wrappedBuffer(bytes))
  }
}

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
class MsgPackCodec extends CodecFactory[RpcRequest, RpcResponse] {
  def client = Function.const {
    new Codec[RpcRequest, RpcResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("response-decoder", new MsgPackDecoder(classOf[RpcResponse]))
          pipeline.addLast("request-encoder", new MsgPackEncoder)
          pipeline
        }
      }
    }
  }

  def server = Function.const {
    new Codec[RpcRequest, RpcResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("request-decoder", new MsgPackDecoder(classOf[RpcRequest]))
          pipeline.addLast("response-encoder", new MsgPackEncoder)
          pipeline
        }
      }
    }
  }
}
