package io.github.dmirib.finagle.msgpack

import com.twitter.finagle.{Codec, CodecFactory}
import org.jboss.netty.channel.{Channel, ChannelHandlerContext, Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.oneone.{OneToOneDecoder, OneToOneEncoder}
import org.jboss.netty.buffer.ChannelBuffers
import io.github.dmitrib.finagle.msgpack.{MsgPackRequest, MsgPackResponse}
import org.msgpack.MessagePack


class ClientDecoder extends OneToOneDecoder {
  def decode(ctx: ChannelHandlerContext, channel: Channel, msg: scala.Any): AnyRef = ???
}

class ClientEncoder extends OneToOneEncoder {
  val msgpack = new MessagePack

  def encode(ctx: ChannelHandlerContext, channel: Channel, msg: scala.Any): AnyRef = {
    if (!msg.isInstanceOf[MsgPackRequest]) {
      return msg
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
      return msg
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
  def client = new Codec {
    def pipelineFactory: ChannelPipelineFactory = {
      val pipeline = Channels.pipeline()
      pipeline.addLast("decoder", new ClientDecoder)
      pipeline.addLast("encoder", new ClientEncoder)
      pipeline
    }
  }

  def server = new Codec {
    def pipelineFactory: ChannelPipelineFactory = {
      val pipeline = Channels.pipeline()
      pipeline.addLast("decoder", new ServerDecoder)
      pipeline.addLast("encoder", new ServerEncoder)
      pipeline
    }
  }
}
