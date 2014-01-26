package io.github.dmitrib.finagle.msgpack

import java.lang.reflect.{Method, InvocationHandler}
import com.twitter.util.Await
import com.twitter.finagle.Service
import com.twitter.util.Future

object ClientProxy {
  def apply[T](client: Service[RpcRequest, RpcResponse],
               id: String,
               interface: Class[T]): T = {
    //TODO: make call to check that the service with requested id exists on server
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
    //TODO: separate method signature types and args types
    val paramTypes = args.map(_.getClass)
    val request = new RpcRequest(method.getName, serviceId, args, paramTypes)

    val responseF = client(request) map { (response) =>
      if (response.failed) {
        response.response match {
          case exception: Exception =>
            throw exception
          case r =>
            throw new RpcException(
              s"response is marked as failed but response is not exception but ${r.getClass}"
            )
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
