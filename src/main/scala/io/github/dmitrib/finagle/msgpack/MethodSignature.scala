package io.github.dmitrib.finagle.msgpack

import java.lang.reflect.Method
import java.security.MessageDigest

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
object MethodSignature {
  def generate(m: Method) = {
    val digest = MessageDigest.getInstance("MD5")
    digest.update(m.getName.getBytes)
    m.getParameterTypes.foreach { t =>
      digest.update(t.getName.getBytes)
    }
    digest.digest().map("%02x".format(_)).mkString
  }
}
