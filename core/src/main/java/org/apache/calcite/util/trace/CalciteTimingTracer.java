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

import org.apache.calcite.util.NumberUtil;

import org.slf4j.Logger;

import java.text.DecimalFormat;

/**
 * CalciteTimingTracer provides a mechanism for tracing the timing of a call
 * sequence at nanosecond resolution.
 */
public class CalciteTimingTracer {
  //~ Static fields/initializers ---------------------------------------------

  private static final DecimalFormat DECIMAL_FORMAT =
      NumberUtil.decimalFormat("###,###,###,###,###");

  //~ Instance fields --------------------------------------------------------

  private final Logger logger;

  private long lastNanoTime;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new timing tracer, publishing an initial event (at elapsed time
   * 0).
   *
   * @param logger     logger on which to log timing events; level FINE will be
   *                   used
   * @param startEvent event to trace as start of timing
   */
  public CalciteTimingTracer(
      Logger logger,
      String startEvent) {
    if (!logger.isDebugEnabled()) {
      this.logger = null;
      return;
    } else {
      this.logger = logger;
    }
    lastNanoTime = System.nanoTime();
    logger.debug("{}:  elapsed nanos=0", startEvent);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Publishes an event with the time elapsed since the previous event.
   *
   * @param event event to trace
   */
  public void traceTime(String event) {
    if (logger == null) {
      return;
    }
    long newNanoTime = System.nanoTime();
    long elapsed = newNanoTime - lastNanoTime;
    lastNanoTime = newNanoTime;
    logger.debug("{}:  elapsed nanos={}", event, DECIMAL_FORMAT.format(elapsed));
  }
}

// End CalciteTimingTracer.java
