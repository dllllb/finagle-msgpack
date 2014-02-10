package io.github.dmitrib.finagle.msgpack

import java.lang.reflect.{Method, InvocationHandler}
import com.twitter.util.Await
import com.twitter.finagle.Service
import com.twitter.util.Future
import scala.Option

object ClientProxy {
  def apply[T](client: Service[RpcRequest, RpcResponse],
               id: String,
               interface: Class[T]): T = {
    val proxy = new ClientProxy(client, id)
    java.lang.reflect.Proxy.newProxyInstance(
      interface.getClassLoader,
      Array(interface),
      proxy
    ).asInstanceOf[T]
  }
}

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
class ClientProxy(val client: Service[RpcRequest, RpcResponse],
                  serviceId: String) extends InvocationHandler {
  def invoke(proxy: scala.Any, method: Method, args: Array[AnyRef]): AnyRef = {
    val safeArgs = Option(args).getOrElse(Array.empty)
    val signature = method.getParameterTypes
    val request = new RpcRequest(method.getName, serviceId, safeArgs, signature)

    val responseF = client(request) map { (response) =>
      if (response.failed) {
        response.response match {
          case ExceptionTransportWrapper(e) => {
            throw e
          }
          case r => {
            throw new RpcException(
              s"response is marked as failed but response is not exception but ${r.getClass}"
            )
          }
        }
      }

      response.response
    }

    if (method.getReturnType.isAssignableFrom(classOf[Future[_]])) {
      responseF
    } else {
      val response = Await.result(responseF)
      response
    }
  }
}
