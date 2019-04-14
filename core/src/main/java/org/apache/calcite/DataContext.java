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
package org.apache.calcite;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.advise.SqlAdvisor;

import com.google.common.base.CaseFormat;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Modifier;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runtime context allowing access to the tables in a database.
 */
public interface DataContext {
  ParameterExpression ROOT =
      Expressions.parameter(Modifier.FINAL, DataContext.class, "root");

  /**
   * Returns a sub-schema with a given name, or null.
   */
  SchemaPlus getRootSchema();

  /**
   * Returns the type factory.
   */
  JavaTypeFactory getTypeFactory();

  /**
   * Returns the query provider.
   */
  QueryProvider getQueryProvider();

  /**
   * Returns a context variable.
   *
   * <p>Supported variables include: "sparkContext", "currentTimestamp",
   * "localTimestamp".</p>
   *
   * @param name Name of variable
   */
  Object get(String name);

  /** Variable that may be asked for in a call to {@link DataContext#get}. */
  enum Variable {
    UTC_TIMESTAMP("utcTimestamp", Long.class),

    /** The time at which the current statement started executing. In
     * milliseconds after 1970-01-01 00:00:00, UTC. Required. */
    CURRENT_TIMESTAMP("currentTimestamp", Long.class),

    /** The time at which the current statement started executing. In
     * milliseconds after 1970-01-01 00:00:00, in the time zone of the current
     * statement. Required. */
    LOCAL_TIMESTAMP("localTimestamp", Long.class),

    /** The Spark engine. Available if Spark is on the class path. */
    SPARK_CONTEXT("sparkContext", Object.class),

    /** A mutable flag that indicates whether user has requested that the
     * current statement be canceled. Cancellation may not be immediate, but
     * implementations of relational operators should check the flag fairly
     * frequently and cease execution (e.g. by returning end of data). */
    CANCEL_FLAG("cancelFlag", AtomicBoolean.class),

    /** Query timeout in milliseconds.
     * When no timeout is set, the value is 0 or not present. */
    TIMEOUT("timeout", Long.class),

    /** Advisor that suggests completion hints for SQL statements. */
    SQL_ADVISOR("sqlAdvisor", SqlAdvisor.class),

    /** Writer to the standard error (stderr). */
    STDERR("stderr", OutputStream.class),

    /** Reader on the standard input (stdin). */
    STDIN("stdin", InputStream.class),

    /** Writer to the standard output (stdout). */
    STDOUT("stdout", OutputStream.class),

    /** Locale in which the current statement is executing.
     * Affects the behavior of functions such as {@code DAYNAME} and
     * {@code MONTHNAME}. Required; defaults to the root locale if the
     * connection does not specify a locale. */
    LOCALE("locale", Locale.class),

    /** Time zone in which the current statement is executing. Required;
     * defaults to the time zone of the JVM if the connection does not specify a
     * time zone. */
    TIME_ZONE("timeZone", TimeZone.class),

    /** The query user.
     *
     * <p>Default value is "sa". */
    USER("user", String.class),

    /** The system user.
     *
     * <p>Default value is "user.name" from
     * {@link System#getProperty(String)}. */
    SYSTEM_USER("systemUser", String.class);

    public final String camelName;
    public final Class clazz;

    Variable(String camelName, Class clazz) {
      this.camelName = camelName;
      this.clazz = clazz;
      assert camelName.equals(
          CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name()));
    }

    /** Returns the value of this variable in a given data context. */
    public <T> T get(DataContext dataContext) {
      //noinspection unchecked
      return (T) clazz.cast(dataContext.get(camelName));
    }
  }
}

// End DataContext.java
