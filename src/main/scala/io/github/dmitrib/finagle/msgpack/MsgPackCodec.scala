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
import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream, ByteArrayOutputStream}

@Message
case class SerializableTransportWrapper(var obj: AnyRef) extends MessagePackable {
  //for msgpack serialization
  def this() = this(null)

  def readFrom(u: Unpacker) {
    val bar = u.readByteArray()
    val bais = new ByteArrayInputStream(bar)
    val ois = new ObjectInputStream(bais)
    obj = ois.readObject()
  }

  def writeTo(pk: Packer) {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    val bar = baos.toByteArray
    pk.write(bar)
  }
}

@Message
case class RpcRequest(var signature: String,
                      var serviceId: String,
                      var args: Seq[AnyRef]) extends MessagePackable {
  //for msgpack serialization
  def this() = this(null, null, null)

  def callId = s"$serviceId:$signature"

  def readFrom(u: Unpacker) {
    signature = u.readString()
    serviceId = u.readString()

    val argClasses = (0 until u.readArrayBegin()) map { i =>
      val typeName = u.readString()
      if (typeName == "null") {
        None
      } else {
        try {
          Some(Class.forName(typeName))
        } catch {
          case e: Exception => throw {
            new RpcException("can't deserialize message", e)
          }
        }
      }
    }
    u.readArrayEnd()

    u.readArrayBegin()
    args = argClasses map {
      case Some(klass) => {
        val value = u.read(klass.asInstanceOf[Class[AnyRef]])
        value
      }
      case _ => null
    }
    u.readArrayEnd()
  }

  def writeTo(pk: Packer) {
    pk.write(signature)
    pk.write(serviceId)

    pk.writeArrayBegin(args.size)
    args.map(Option(_).map(_.getClass.getName).getOrElse("null")).foreach(pk.write)
    pk.writeArrayEnd()

    val notNullArgs = args.filter(_ != null)
    pk.writeArrayBegin(notNullArgs.size)
    notNullArgs.foreach(pk.write)
    pk.writeArrayEnd()
  }
}

@Message
case class RpcResponse(var response: AnyRef, var failed: Boolean) extends MessagePackable {
  //for msgpack serialization
  def this() = this(null, false)

  def readFrom(u: Unpacker) {
    val typeName = u.readString()
    response = if (typeName == "null") {
      null
    } else {
      val klass = try {
        Class.forName(typeName)
      } catch {
        case e: Exception => throw {
          new RpcException("can't deserialize message", e)
        }
      }
      u.read(klass.asInstanceOf[Class[AnyRef]])
    }
    failed = u.readBoolean()
  }

  def writeTo(pk: Packer) {
    pk.write(Option(response).map(_.getClass.getName).getOrElse("null"))
    Option(response).foreach(pk.write)
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
