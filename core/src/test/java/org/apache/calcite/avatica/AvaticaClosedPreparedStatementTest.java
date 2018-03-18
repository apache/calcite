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
package org.apache.calcite.avatica;

import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLType;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
/**
 * Tests for {@code AvaticaPreparedStatement} relative to close behavior
 */
@RunWith(Parameterized.class)
public class AvaticaClosedPreparedStatementTest extends AvaticaClosedTestBase<PreparedStatement> {
  // Mapping between Connection method and the verifier to check close behavior
  private static final Function<Method, MethodVerifier> METHOD_MAPPING = method -> {
    String name = method.getName();
    Class<?>[] parameterTypes = method.getParameterTypes();
    switch (name) {
    // Those methods are not supported yet
    case "setObject":
      if (parameterTypes.length >= 3 && parameterTypes[2] == SQLType.class) {
        return ASSERT_UNSUPPORTED;
      }
      return ASSERT_CLOSED;

    default:
      // Delegate to AvaticaClosedStatementTest as AvaticaPreparedStatement is
      // a subclass of AvaticaStatement
      return AvaticaClosedStatementTest.METHOD_MAPPING.apply(method);
    }
  };

  @Parameters(name = "{index}: {0}")
  public static Iterable<? extends Object[]> getParameters() {
    return Stream.<Object[]>concat(
        getMethodsToTest(Statement.class, AvaticaPreparedStatement.class, METHOD_FILTER,
            METHOD_MAPPING).stream(),
        getMethodsToTest(PreparedStatement.class, AvaticaPreparedStatement.class, METHOD_FILTER,
            METHOD_MAPPING).stream())
        .collect(Collectors.toList());
  }

  public AvaticaClosedPreparedStatementTest(Method method, MethodVerifier verifier) {
    super(method, verifier);
  }

  @Override protected PreparedStatement newInstance() throws Exception {
    UnregisteredDriver driver = new TestDriver();
    AvaticaConnection connection = new AvaticaConnection(driver, driver.createFactory(),
        "jdbc:avatica", new Properties()) {
    };
    StatementHandle handle = mock(StatementHandle.class);
    Signature signature = new Signature(Collections.emptyList(), "", Collections.emptyList(),
        Collections.emptyMap(), null, Meta.StatementType.SELECT);
    AvaticaPreparedStatement preparedStatement = new AvaticaPreparedStatement(connection, handle,
        signature, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT) {

    };

    preparedStatement.close();
    assertTrue("Prepared statement is not closed", preparedStatement.isClosed());
    return preparedStatement;
  }
}

// End AvaticaClosedPreparedStatementTest.java
