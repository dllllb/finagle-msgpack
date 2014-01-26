package io.github.dmitrib.finagle.msgpack

import org.junit._
import org.junit.Assert._
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory
import org.jboss.netty.channel.local.LocalAddress
import com.twitter.util.{Future, Await}
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import java.util.concurrent.Executors
import org.msgpack.annotation.Message
import java.util.logging.Logger
import org.slf4j.bridge.SLF4JBridgeHandler

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
  def raise(): Int
  def next(n: Number): Number
}

class TestException(msg: String) extends RuntimeException(msg)

class TestServiceImpl extends TestService {
  def sum(first: Point, second: Point) = {
    Point(first.x + second.x, first.y + second.y)
  }

  def sum(first: Int, second: Int) = first+second

  def raise() = throw new TestException("failure")

  def next(n: Number) = new java.lang.Long(n.longValue() +1)
}

object ClientTest {
  @BeforeClass def setupLogging() {
    SLF4JBridgeHandler.removeHandlersForRootLogger()
    SLF4JBridgeHandler.install()
  }
}

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
class ClientTest {
  val serverService = new RpcServer(
    Map(
      "test" -> new TestServiceImpl
    ),
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
    .logger(Logger.getLogger("finagle-client"))
    .build()

  val service = ClientProxy(client, "test", classOf[TestService])

  @Test def testCall() {
    val res = service.sum(Point(1, 1), Point(2, 2))
    assertEquals(Point(3, 3), res)
  }

  @Test def testCallPrimitiveTypes() {
    val res = service.sum(1, 2)
    assertEquals(3, res)
  }

  @Test(expected = classOf[RpcException]) def testWrongServiceId() {
    ClientProxy(client, "test-x", classOf[TestService])
  }

  @Test(expected = classOf[TestException]) def testServerSideExcetion() {
    service.raise()
  }

  @Test def testAsyncCall() {
    val asyncService = ClientProxy(client, "test", classOf[AsyncTestService])
    val resF = asyncService.sum(Point(1, 1), Point(2, 2))
    val res = Await.result(resF)
    assertEquals(Point(3, 3), res)
  }

  @Test def testBaseTypesInMethodDeclaration() {
    val res = service.next(new Integer(1)).longValue()
    assertEquals(2, res)
  }

  @After def close() {
    Await.result(client.close())
    Await.result(server.close())
  }
}
