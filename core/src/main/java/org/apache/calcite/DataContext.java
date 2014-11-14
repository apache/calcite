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
package net.hydromatic.optiq;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.expressions.ParameterExpression;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.eigenbase.sql.advise.SqlAdvisor;
import org.eigenbase.util.Util;

import java.lang.reflect.Modifier;
import java.util.TimeZone;

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

    /** Sql advisor that suggests completion hints. */
    SQL_ADVISOR("sqlAdvisor", SqlAdvisor.class),

    /** Time zone in which the current statement is executing. Required;
     * defaults to the time zone of the JVM if the connection does not specify a
     * time zone. */
    TIME_ZONE("timeZone", TimeZone.class);

    public final String camelName;
    public final Class clazz;

    Variable(String camelName, Class clazz) {
      this.camelName = camelName;
      this.clazz = clazz;
      assert camelName.equals(Util.toCamelCase(name()));
    }

    /** Returns the value of this variable in a given data context. */
    public <T> T get(DataContext dataContext) {
      //noinspection unchecked
      return (T) clazz.cast(dataContext.get(camelName));
    }
  }
}

// End DataContext.java
