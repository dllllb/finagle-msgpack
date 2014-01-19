package io.github.dmitrib.finagle.msgpack

import com.twitter.finagle.Service
import com.twitter.util.{FuturePool, Future}
import java.util.concurrent.ExecutorService

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
class RpcServer(val handlers: Map[String, AnyRef],
                executorService: ExecutorService)
  extends Service[RpcRequest, RpcResponse] with Logging {

  private val futurePool = FuturePool(executorService)

  def apply(request: RpcRequest): Future[RpcResponse] = {

    log.debug(s"RPC.call ${request.getCallId}")

    futurePool {
      try {
        val handler = handlers.getOrElse(
          request.getServiceId,
          throw new RpcException(s"service with ID ${request.getServiceId} does not exist")
        )

        val method = try {
          handler.getClass.getMethod(request.getMethod, request.getParamTypes:_*)
        } catch {
          case e: Exception => {
            throw new RpcException(
              s"can't find method ${request.getMethod} in service '${request.getServiceId}'",
              e
            )
          }
        }

        val res = method.invoke(handler, request.getArgs:_*)
        log.debug(s"RPC.return ${request.getCallId} -> $res")
        new RpcResponse(res, failed=false)
      } catch {
        case e: Exception => {
          log.debug(s"RPC.exception ${request.getCallId} -> $e")
          new RpcResponse(e, failed=true)
        }
      }
    }
  }
}