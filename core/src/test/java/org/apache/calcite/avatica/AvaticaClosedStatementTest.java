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

import org.apache.calcite.avatica.Meta.StatementHandle;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.function.Function;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@code AvaticaStatement} relative to close behavior
 */
@RunWith(Parameterized.class)
public class AvaticaClosedStatementTest extends AvaticaClosedTestBase<Statement> {
  // Mapping between Connection method and the verifier to check close behavior
  static final Function<Method, MethodVerifier> METHOD_MAPPING = method -> {
    String name = method.getName();
    Class<?>[] parameterTypes = method.getParameterTypes();
    switch (name) {
    // Those methods are not supported yet
    case "getGeneratedKeys":
    case "setCursorName":
    case "setEscapeProcessing":
    case "setPoolable":
      return ASSERT_UNSUPPORTED;

    // Those methods are partially supported
    case "execute":
    case "executeUpdate":
    case "executeLargeUpdate":
      if (parameterTypes.length == 2
          && (parameterTypes[1] == int.class || parameterTypes[1] == int[].class
              || parameterTypes[1] == String[].class)) {
        return ASSERT_UNSUPPORTED;
      }
      return ASSERT_CLOSED;

    default:
      return ASSERT_CLOSED;
    }
  };

  @Parameters(name = "{index}: {0}")
  public static Iterable<? extends Object[]> getParameters() {
    return getMethodsToTest(Statement.class, AvaticaStatement.class, METHOD_FILTER, METHOD_MAPPING);
  }

  public AvaticaClosedStatementTest(Method method, MethodVerifier verifier) {
    super(method, verifier);
  }

  @Override protected Statement newInstance() throws Exception {
    UnregisteredDriver driver = new TestDriver();
    AvaticaConnection connection = new AvaticaConnection(driver, driver.createFactory(),
        "jdbc:avatica", new Properties()) {
    };
    StatementHandle handle = mock(StatementHandle.class);
    AvaticaStatement statement = new AvaticaStatement(connection, handle,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT) {
    };
    statement.close();
    assertTrue("Statement is not closed", statement.isClosed());

    return statement;
  }
}

// End AvaticaClosedStatementTest.java
