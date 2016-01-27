/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.util.trace;

import org.slf4j.Logger;

/**
 * Small extension to {@link Logger} with some performance improvements.
 *
 * <p>{@link Logger#info(String format, Object[] params)} is expensive
 * to call, since the caller must always allocate and fill in the array
 * <code>params</code>, even when the <code>level</code> will prevent a message
 * being logged. On the other hand, {@link Logger#info(String msg)}
 * and {@link Logger#info(String msg, Object o)} do not have this
 * problem.
 *
 * <p>As a workaround this class provides
 * {@link #info(String msg, Object o1, Object o2)} etc. (The varargs feature of
 * java 1.5 half-solves this problem, by automatically wrapping args in an
 * array, but it does so without testing the level.)
 *
 * <p>Usage: replace:
 *
 * <blockquote><code>static final Logger tracer =
 * CalciteTracer.getMyTracer();</code></blockquote>
 *
 * <p>by:
 *
 * <blockquote><code>static final CalciteLogger tracer =
 *     new CalciteLogger(CalciteTrace.getMyTracer());</code></blockquote>
 */
public class CalciteLogger {
  //~ Instance fields --------------------------------------------------------

  private final Logger logger; // delegate

  //~ Constructors -----------------------------------------------------------

  public CalciteLogger(Logger logger) {
    assert logger != null;
    this.logger = logger;
  }

  //~ Methods ----------------------------------------------------------------

  // WARN

  /**
   * Logs a WARN message with two Object parameters
   */
  public void warn(String format, Object arg1, Object arg2) {
    // slf4j already avoids the array creation for 1 or 2 arg invocations
    logger.warn(format, arg1, arg2);
  }

  /**
   * Conditionally logs a WARN message with three Object parameters
   */
  public void warn(String format, Object arg1, Object arg2, Object arg3) {
    if (logger.isWarnEnabled()) {
      logger.warn(format, arg1, arg2, arg3);
    }
  }

  /**
   * Conditionally logs a WARN message with four Object parameters
   */
  public void warn(String format, Object arg1, Object arg2, Object arg3, Object arg4) {
    if (logger.isWarnEnabled()) {
      logger.warn(format, arg1, arg2, arg3, arg4);
    }
  }

  public void warn(String format, Object... args) {
    if (logger.isWarnEnabled()) {
      logger.warn(format, args);
    }
  }

  // INFO

  /**
   * Logs an INFO message with two Object parameters
   */
  public void info(String format, Object arg1, Object arg2) {
    // slf4j already avoids the array creation for 1 or 2 arg invocations
    logger.info(format, arg1, arg2);
  }

  /**
   * Conditionally logs an INFO message with three Object parameters
   */
  public void info(String format, Object arg1, Object arg2, Object arg3) {
    if (logger.isInfoEnabled()) {
      logger.info(format, arg1, arg2, arg3);
    }
  }

  /**
   * Conditionally logs an INFO message with four Object parameters
   */
  public void info(String format, Object arg1, Object arg2, Object arg3, Object arg4) {
    if (logger.isInfoEnabled()) {
      logger.info(format, arg1, arg2, arg3, arg4);
    }
  }

  public void info(String format, Object... args) {
    if (logger.isInfoEnabled()) {
      logger.info(format, args);
    }
  }

  // DEBUG

  /**
   * Logs a DEBUG message with two Object parameters
   */
  public void debug(String format, Object arg1, Object arg2) {
    // slf4j already avoids the array creation for 1 or 2 arg invocations
    logger.debug(format, arg1, arg2);
  }

  /**
   * Conditionally logs a DEBUG message with three Object parameters
   */
  public void debug(String format, Object arg1, Object arg2, Object arg3) {
    if (logger.isDebugEnabled()) {
      logger.debug(format, arg1, arg2, arg3);
    }
  }

  /**
   * Conditionally logs a DEBUG message with four Object parameters
   */
  public void debug(String format, Object arg1, Object arg2, Object arg3, Object arg4) {
    if (logger.isDebugEnabled()) {
      logger.debug(format, arg1, arg2, arg3, arg4);
    }
  }

  public void debug(String format, Object... args) {
    if (logger.isDebugEnabled()) {
      logger.debug(format, args);
    }
  }

  // TRACE

  /**
   * Logs a TRACE message with two Object parameters
   */
  public void trace(String format, Object arg1, Object arg2) {
    // slf4j already avoids the array creation for 1 or 2 arg invocations
    logger.trace(format, arg1, arg2);
  }

  /**
   * Conditionally logs a TRACE message with three Object parameters
   */
  public void trace(String format, Object arg1, Object arg2, Object arg3) {
    if (logger.isTraceEnabled()) {
      logger.trace(format, arg1, arg2, arg3);
    }
  }

  /**
   * Conditionally logs a TRACE message with four Object parameters
   */
  public void trace(String format, Object arg1, Object arg2, Object arg3, Object arg4) {
    if (logger.isTraceEnabled()) {
      logger.trace(format, arg1, arg2, arg3, arg4);
    }
  }

  public void trace(String format, Object... args) {
    if (logger.isTraceEnabled()) {
      logger.trace(format, args);
    }
  }

  // We expose and delegate the commonly used part of the Logger interface.
  // For everything else, just expose the delegate. (Could use reflection.)
  public Logger getLogger() {
    return logger;
  }

  // Hold-over from the previous j.u.logging implementation

  public void warn(String msg) {
    logger.warn(msg);
  }

  public void info(String msg) {
    logger.info(msg);
  }
}

// End CalciteLogger.java
