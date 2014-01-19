package io.github.dmitrib.finagle.msgpack

import org.slf4j.{Logger, LoggerFactory}

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
trait Logging {
  protected val log = new RichLogger(LoggerFactory.getLogger(this.getClass))
}

class RichLogger(val logger: Logger) {
  def info(message: String) {
    if (logger.isInfoEnabled)
      logger.info(message)
  }

  def info(throwable: Throwable, message: => String) {
    if (logger.isInfoEnabled)
      logger.info(message, throwable)
  }

  def warn(message: => String) {
    if (logger.isWarnEnabled)
      logger.warn(message)
  }

  def warn(throwable: Throwable, message: => String) {
    if (logger.isWarnEnabled)
      logger.warn(message, throwable)
  }

  def error(message: => String) {
    if (logger.isErrorEnabled)
      logger.error(message)
  }

  def error(throwable: Throwable, message: => String) {
    if (logger.isErrorEnabled)
      logger.error(message, throwable)
  }

  def debug(message: => String) {
    if (logger.isDebugEnabled)
      logger.debug(message)
  }

  def debug(throwable: Throwable, message: => String) {
    if (logger.isDebugEnabled)
      logger.debug(message, throwable)
  }

  def trace(message: => String) {
    if (logger.isTraceEnabled)
      logger.trace(message)
  }

  def trace(throwable: Throwable, message: => String) {
    if (logger.isTraceEnabled)
      logger.trace(message, throwable)
  }
}