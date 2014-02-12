package io.github.dmitrib.finagle.msgpack

import com.twitter.finagle.Service
import com.twitter.util.{FuturePool, Future}
import java.util.concurrent.ExecutorService
import java.lang.reflect.InvocationTargetException

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
class RpcServer(val handlers: Map[String, AnyRef],
                executorService: ExecutorService)
  extends Service[RpcRequest, RpcResponse] with Logging {

  val handlerMethods = handlers.map { case (name, handler) =>
    val methodsBySignature = handler.getClass.getMethods.map { m =>
      (MethodSignature.generate(m), m)
    }.toMap

    (name, methodsBySignature)
  }

  private val futurePool = FuturePool(executorService)

  def apply(request: RpcRequest): Future[RpcResponse] = {

    log.debug(s"RPC.call ${request.callId}")

    futurePool {
      try {
        val handler = handlers.getOrElse(
          request.serviceId,
          throw new RpcException(s"service with ID ${request.serviceId} does not exist")
        )

        val method = handlerMethods.get(request.serviceId).flatMap(_.get(request.signature)).getOrElse {
          throw new RpcException(
            s"can't find method with signature '${request.signature}' in service '${request.serviceId}'"
          )
        }

        val res = method.invoke(handler, request.args:_*)
        log.debug(s"RPC.return ${request.callId} -> $res")
        new RpcResponse(res, failed=false)
      } catch {
        case e: InvocationTargetException => {
          new RpcResponse(SerializableTransportWrapper(e.getTargetException), failed=true)
        }
        case e: Exception => {
          log.debug(s"RPC.exception ${request.callId} -> $e")
          new RpcResponse(SerializableTransportWrapper(e), failed=true)
        }
      }
    }
  }
}