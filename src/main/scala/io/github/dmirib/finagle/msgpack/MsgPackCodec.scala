package io.github.dmirib.finagle.msgpack

import com.twitter.finagle.{Codec, CodecFactory}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.oneone.{OneToOneDecoder, OneToOneEncoder}
import org.jboss.netty.buffer.ChannelBuffers
import org.msgpack.MessagePack

case class MsgPackRequest(args: Array[AnyRef], method: String, serviceId: String)

case class MsgPackResponse(response: AnyRef)

class ClientDecoder extends OneToOneDecoder {
  def decode(ctx: ChannelHandlerContext, channel: Channel, msg: scala.Any): AnyRef = ???
}

class ClientEncoder extends OneToOneEncoder {
  val msgpack = new MessagePack

  def encode(ctx: ChannelHandlerContext, channel: Channel, msg: scala.Any): AnyRef = {
    if (!msg.isInstanceOf[MsgPackRequest]) {
      return msg.asInstanceOf[AnyRef]
    }

    val t = msg.asInstanceOf[MsgPackRequest]

    val bytes = msgpack.write(t)

    val msgLenBuf = ChannelBuffers.buffer(4)
    msgLenBuf.writeInt(bytes.size)

    ChannelBuffers.wrappedBuffer(msgLenBuf, ChannelBuffers.wrappedBuffer(bytes))
  }
}

class ServerEncoder extends OneToOneEncoder {
  val msgpack = new MessagePack

  def encode(ctx: ChannelHandlerContext, channel: Channel, msg: scala.Any): AnyRef = {
    if (!msg.isInstanceOf[MsgPackResponse]) {
      return msg.asInstanceOf[AnyRef]
    }

    val t = msg.asInstanceOf[MsgPackResponse]

    val bytes = msgpack.write(t)

    val msgLenBuf = ChannelBuffers.buffer(4)
    msgLenBuf.writeInt(bytes.size)

    ChannelBuffers.wrappedBuffer(msgLenBuf, ChannelBuffers.wrappedBuffer(bytes))
  }
}

class ServerDecoder extends OneToOneDecoder {
  def decode(ctx: ChannelHandlerContext, channel: Channel, msg: scala.Any): AnyRef = ???
}

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
class MsgPackCodec extends CodecFactory[MsgPackRequest, MsgPackResponse] {
  def client = Function.const {
    new Codec[MsgPackRequest, MsgPackResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("decoder", new ClientDecoder)
          pipeline.addLast("encoder", new ClientEncoder)
          pipeline
        }
      }
    }
  }

  def server = Function.const {
    new Codec[MsgPackRequest, MsgPackResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("decoder", new ServerDecoder)
          pipeline.addLast("encoder", new ServerEncoder)
          pipeline
        }
      }
    }
  }
}
