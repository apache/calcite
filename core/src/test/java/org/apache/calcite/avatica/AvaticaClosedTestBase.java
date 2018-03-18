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

import org.junit.Test;
import org.junit.internal.Throwables;
import org.mockito.Answers;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Base class for tests checking close behavior
 * @param <T> the JDBC interface to test
 */
public abstract class AvaticaClosedTestBase<T extends AutoCloseable> {

  /**
   * A fake test driver for test.
   */
  protected static final class TestDriver extends UnregisteredDriver {

    @Override protected DriverVersion createDriverVersion() {
      return new DriverVersion("test", "test 0.0.0", "test", "test 0.0.0", false, 0, 0, 0, 0);
    }

    @Override protected String getConnectStringPrefix() {
      return "jdbc:test";
    }

    @Override public Meta createMeta(AvaticaConnection connection) {
      return Mockito.mock(Meta.class, Answers.RETURNS_DEEP_STUBS);
    }
  }

  /**
   * Functional interface to invoke JDBC method
   */
  public interface MethodInvocation {
    void invoke() throws Exception;
  }

  /**
   * Functional interface for verifying invoke JDBC method
   */
  public interface MethodVerifier {
    void verify(MethodInvocation invocation) throws Exception;
  }

  /**
   * Verifier for closed methods
   */
  public static final MethodVerifier ASSERT_CLOSED = invocation -> {
    try {
      invocation.invoke();
      fail();
    } catch (SQLException e) {
      assertThat(e, is(instanceOf(SQLException.class)));
      assertThat(e.getMessage(), endsWith(" closed"));
    }
  };

  /**
   * Verifier for unsupported methods
   */
  public static final MethodVerifier ASSERT_UNSUPPORTED = invocation -> {
    try {
      invocation.invoke();
      fail();
    } catch (SQLFeatureNotSupportedException e) {
      // success
    }
  };

  protected static final Predicate<? super Method> METHOD_FILTER = method -> {
    final String name = method.getName();
    switch (name) {
    case "close":
    case "isClosed":
      return false;

    default:
      return true;
    }
  };

  private final Method method;
  private final MethodVerifier verifier;

  public AvaticaClosedTestBase(Method method, MethodVerifier verifier) {
    this.method = method;
    this.verifier = verifier;
  }

  protected abstract T newInstance() throws Exception;

  @Test
  public void checkMethod() throws Exception {
    T object = newInstance();

    final Object[] arguments = createFakeArguments(method);
    verifier.verify(() -> {
      try {
        method.invoke(object, arguments);
      } catch (InvocationTargetException e) {
        throw Throwables.rethrowAsException(e.getCause());
      }
    });
  }

  /**
   * Get the list of methods to test and their associated verifier
   */
  protected static <T> List<? extends Object[]> getMethodsToTest(Class<T> iface,
      Class<? extends T> implementation, Predicate<? super Method> methodFilter,
      Function<? super Method, ? extends MethodVerifier> methodMapping) {
    // Get all the methods from the class, exclude the ones which are working even if object
    // is closed and find their expected behavior
    Stream<Method> methods = Arrays.stream(iface.getDeclaredMethods());

    return methods
        .filter(m -> {
          // Filter out methods which are not overridden in implementation
          try {
            return !implementation.getMethod(m.getName(), m.getParameterTypes()).isDefault();
          } catch (NoSuchMethodException e) {
            // if implementation has not a method, although class is not abstract,
            // assume that it's because parameters don't match exactly
            return true;
          }
        })
        .filter(methodFilter).map(m -> new Object[] { m, methodMapping.apply(m) })
        .collect(Collectors.toList());
  }

  /**
   * Create an array of default arguments for the method
   */
  private static Object[] createFakeArguments(Method method) {
    Class<?>[] parameterTypes = method.getParameterTypes();
    Object[] values = new Object[parameterTypes.length];
    for (int i = 0; i < parameterTypes.length; i++) {
      Class<?> parameterType = parameterTypes[i];
      // Primitive types
      if (parameterType == Boolean.TYPE) {
        values[i] = Boolean.FALSE;
      } else if (parameterType == Byte.TYPE) {
        values[i] = Byte.valueOf((byte) 0);
      } else if (parameterType == Short.TYPE) {
        values[i] = Short.valueOf((short) 0);
      } else if (parameterType == Integer.TYPE) {
        values[i] = Integer.valueOf(0);
      } else if (parameterType == Long.TYPE) {
        values[i] = Long.valueOf(0);
      } else if (parameterType == Float.TYPE) {
        values[i] = Float.valueOf(0);
      } else if (parameterType == Double.TYPE) {
        values[i] = Double.valueOf(0);
      } else if (parameterType == Character.TYPE) {
        values[i] = Character.valueOf((char) 0);
      } else {
        values[i] = null;
      }
    }

    return values;
  }
}

// End AvaticaClosedTestBase.java
