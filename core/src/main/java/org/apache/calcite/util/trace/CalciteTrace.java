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

import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelImplementorImpl;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Contains all of the {@link java.util.logging.Logger tracers} used within
 * org.apache.calcite class libraries.
 *
 * <h3>Note to developers</h3>
 *
 * <p>Please ensure that every tracer used in org.apache.calcite is added to
 * this class as a <em>public static final</em> member called <code>
 * <i>component</i>Tracer</code>. For example, {@link #getPlannerTracer} is the
 * tracer used by all classes which take part in the query planning process.
 *
 * <p>The javadoc in this file is the primary source of information on what
 * tracers are available, so the javadoc against each tracer member must be an
 * up-to-date description of what that tracer does. Be sure to describe what
 * {@link Level tracing level} is required to obtain each category of tracing.
 *
 * <p>In the class where the tracer is used, create a <em>private</em> (or
 * perhaps <em>protected</em>) <em>static final</em> member called <code>
 * tracer</code>.
 */
public abstract class CalciteTrace {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * The "org.apache.calcite.sql.parser" tracer reports parser events in
   * {@link org.apache.calcite.sql.parser.SqlParser} and other classes (at level
   * {@link Level#FINE} or higher).
   */
  public static final Logger PARSER_LOGGER = getParserTracer();

  private static final ThreadLocal<Function2<Void, File, String>>
  DYNAMIC_HANDLER =
      new ThreadLocal<Function2<Void, File, String>>() {
        @Override protected Function2<Void, File, String> initialValue() {
          return Functions.ignore2();
        }
      };

  //~ Methods ----------------------------------------------------------------

  /**
   * The "org.apache.calcite.plan.RelOptPlanner" tracer prints the query
   * optimization process.
   *
   * <p>Levels:
   *
   * <ul>
   * <li>{@link Level#FINE} prints rules as they fire;
   * <li>{@link Level#FINER} prints and validates the whole expression pool
   * and rule queue as each rule fires;
   * <li>{@link Level#FINEST} prints finer details like rule importances.
   * </ul>
   */
  public static Logger getPlannerTracer() {
    return Logger.getLogger(RelOptPlanner.class.getName());
  }

  /**
   * The "org.apache.calcite.prepare.Prepare" tracer prints the generated
   * program at level {@link java.util.logging.Level#FINE} or higher.
   */
  public static Logger getStatementTracer() {
    return Logger.getLogger(Prepare.class.getName());
  }

  /**
   * The "org.apache.calcite.rel.RelImplementorImpl" tracer reports when
   * expressions are bound to variables ({@link Level#FINE})
   */
  public static Logger getRelImplementorTracer() {
    return Logger.getLogger(RelImplementorImpl.class.getName());
  }

  /**
   * The tracer "org.apache.calcite.sql.timing" traces timing for
   * various stages of query processing.
   *
   * @see CalciteTimingTracer
   */
  public static Logger getSqlTimingTracer() {
    return Logger.getLogger("org.apache.calcite.sql.timing");
  }

  /**
   * The "org.apache.calcite.sql.parser" tracer reports parse events.
   */
  public static Logger getParserTracer() {
    return Logger.getLogger("org.apache.calcite.sql.parser");
  }

  /**
   * The "org.apache.calcite.sql2rel" tracer reports parse events.
   */
  public static Logger getSqlToRelTracer() {
    return Logger.getLogger("org.apache.calcite.sql2rel");
  }

  /**
   * Thread-local handler that is called with dynamically generated Java code.
   * It exists for unit-testing.
   * The handler is never null; the default handler does nothing.
   */
  public static ThreadLocal<Function2<Void, File, String>> getDynamicHandler() {
    return DYNAMIC_HANDLER;
  }
}

// End CalciteTrace.java
