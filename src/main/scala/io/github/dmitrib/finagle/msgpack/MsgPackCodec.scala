package io.github.dmitrib.finagle.msgpack

import com.twitter.finagle.{Codec, CodecFactory}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.msgpack.MessagePack
import org.jboss.netty.handler.codec.frame.FrameDecoder

class MsgPackDecoder extends FrameDecoder {
  val msgpack = new MessagePack

  def decode(ctx: ChannelHandlerContext, channel: Channel, buf: ChannelBuffer): AnyRef = {
    if (buf.readableBytes() < 4) {
      return null
    }

    buf.markReaderIndex()
    val msgLen = buf.readInt()


    if (buf.readableBytes() < msgLen) {
      buf.resetReaderIndex()
      return null
    }

    val msgBuf = new Array[Byte](msgLen)
    buf.readBytes(msgLen)
    //TODO: use msgpack unpacker
    val value = msgpack.read(msgBuf)
    value
  }
}

class MsgPackEncoder extends OneToOneEncoder {
  val msgpack = new MessagePack

  def encode(ctx: ChannelHandlerContext, channel: Channel, msg: scala.Any): AnyRef = {
    //TODO: use msgpack packer
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
          pipeline.addLast("decoder", new MsgPackDecoder)
          pipeline.addLast("encoder", new MsgPackEncoder)
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
          pipeline.addLast("decoder", new MsgPackDecoder)
          pipeline.addLast("encoder", new MsgPackEncoder)
          pipeline
        }
      }
    }
  }
}
