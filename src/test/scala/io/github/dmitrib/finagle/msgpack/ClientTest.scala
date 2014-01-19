package io.github.dmitrib.finagle.msgpack

import org.junit.Test
import org.junit.Assert._
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory
import org.jboss.netty.channel.local.LocalAddress
import com.twitter.util.{Future, Await}
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import java.util.concurrent.Executors
import org.msgpack.annotation.Message

@Message
case class Point(var x: Int, var y: Int) {
  //for msgpack serialization
  def this() = this(0, 0)
}

trait AsyncTestService {
  def sum(first: Point, second: Point): Future[Point]
}

trait TestService {
  def sum(first: Point, second: Point): Point
  def sum(first: Int, second: Int): Int
  def raise: Int
}

class TestServiceImpl extends TestService {
  def sum(first: Point, second: Point) = {
    Point(first.x + second.x, first.y + second.y)
  }

  def sum(first: Int, second: Int) = first+second

  def raise = throw new RuntimeException("failure")
}

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
class ClientTest {
  @Test def testCall() {
    val serverService = new RpcServer(
      Map("test" -> new TestServiceImpl),
      Executors.newFixedThreadPool(2)
    )

    val server = ServerBuilder()
      .bindTo(new LocalAddress(1))
      .channelFactory(new DefaultLocalServerChannelFactory)
      .codec(new MsgPackCodec)
      .name("MsgPackServer")
      .build(serverService)

    val client = ClientBuilder()
      .hosts(Seq(new LocalAddress(1)))
      .channelFactory(new DefaultLocalClientChannelFactory)
      .codec(new MsgPackCodec())
      .hostConnectionLimit(1)
      .build()

    val service = ClientProxy(client, "test", classOf[TestService])

    val res = service.sum(1, 2)
    assertEquals(3, res)

    val res2 = service.sum(Point(1, 1), Point(2, 2))
    assertEquals(Point(3, 3), res2)

    Await.result(client.close())
    Await.result(server.close())
  }

  //TODO: method with exception test
  //TODO: wrong service id test
  //TODO: async call with future as return type test
}
