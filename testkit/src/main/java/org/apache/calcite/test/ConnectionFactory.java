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
package org.apache.calcite.test;

import org.apache.calcite.avatica.ConnectionProperty;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Creates JDBC connections for tests.
 *
 * <p>The base class is abstract, and all of the {@code with} methods throw.
 *
 * <p>Avoid creating new sub-classes otherwise it would be hard to support
 * {@code .with(property, value).with(...)} kind of chains.
 *
 * <p>If you want augment the connection, use
 * {@link CalciteAssert.ConnectionPostProcessor}.
 *
 * @see ConnectionFactories
 */
public interface ConnectionFactory {
  Connection createConnection() throws SQLException;

  default ConnectionFactory with(String property, Object value) {
    throw new UnsupportedOperationException();
  }

  default ConnectionFactory with(ConnectionProperty property, Object value) {
    throw new UnsupportedOperationException();
  }

  default ConnectionFactory with(CalciteAssert.ConnectionPostProcessor postProcessor) {
    throw new UnsupportedOperationException();
  }
}
