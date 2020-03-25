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
package org.apache.calcite.util;

import org.apache.calcite.linq4j.function.Parameter;

import org.apache.commons.lang3.StringUtils;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Unit test for {@link ReflectUtil}.
 */
public class ReflectUtilTest {

  @Test public void testGetBoxingClass() {
    checkGetBoxingClass(PrimitiveType.BYTE);
    checkGetBoxingClass(PrimitiveType.CHAR);
    checkGetBoxingClass(PrimitiveType.SHORT);
    checkGetBoxingClass(PrimitiveType.INT);
    checkGetBoxingClass(PrimitiveType.LONG);
    checkGetBoxingClass(PrimitiveType.FLOAT);
    checkGetBoxingClass(PrimitiveType.DOUBLE);
  }

  @Test public void testGetByteBufferReadAndWriteMethod() {
    checkGetByteBufferReadAndWriteMethod(PrimitiveType.BYTE);
    checkGetByteBufferReadAndWriteMethod(PrimitiveType.CHAR);
    checkGetByteBufferReadAndWriteMethod(PrimitiveType.SHORT);
    checkGetByteBufferReadAndWriteMethod(PrimitiveType.INT);
    checkGetByteBufferReadAndWriteMethod(PrimitiveType.LONG);
    checkGetByteBufferReadAndWriteMethod(PrimitiveType.FLOAT);
    checkGetByteBufferReadAndWriteMethod(PrimitiveType.DOUBLE);
  }

  @Test public void testGetUnqualifiedClassName() {
    assertThat(ReflectUtil.getUnqualifiedClassName(String.class), is("String"));
    assertThat(ReflectUtil.getUnqualifiedClassName(ReflectUtilTest.Car.class),
        is("ReflectUtilTest$Car"));
  }

  @Test public void testGetUnmangledMethodName() {
    assertThat(ReflectUtil.getUnmangledMethodName(getMethod(String.class, "substring", int.class)),
        is("java.lang.String.substring(int)"));
    assertThat(ReflectUtil.getUnmangledMethodName(getMethod(Car.class, "drive", String.class)),
        is("org.apache.calcite.util.ReflectUtilTest$Car.drive(java.lang.String)"));
  }

  @Test public void testGetUnmangledMethodNameFullSignature() {
    assertThat(
        ReflectUtil.getUnmangledMethodName(String.class, "substring", new Class[]{int.class}),
        is("java.lang.String.substring(int)"));
    assertThat(
        ReflectUtil.getUnmangledMethodName(Car.class, "drive", new Class[]{String.class}),
        is("org.apache.calcite.util.ReflectUtilTest$Car.drive(java.lang.String)"));
  }

  @Test public void testGetParameterName() {
    assertThat(ReflectUtil.getParameterName(getMethod(Car.class, "drive", String.class), 0),
        is("arg0"));
  }

  @Test public void testIsParameterOptional() {
    assertThat(
        ReflectUtil.isParameterOptional(
            getMethod(Person.class, "say", String.class, String.class), 0),
                is(true));
    assertThat(
        ReflectUtil.isParameterOptional(
            getMethod(Person.class, "say", String.class, String.class), 1),
                is(false));
  }

  @Test public void testCreateMethodDispatcherAndInvoke() {
    ReflectUtil.MethodDispatcher<Result> dispatcher = getResultMethodDispatcher();
    Delta delta1 = new IntDelta(20);
    Result result1 = dispatcher.invoke(delta1);
    assertThat(result1.result, is(30L));

    Delta delta2 = new LongDelta(50L);
    Result result2 = dispatcher.invoke(delta2);
    assertThat(result2.result, is(80L));

    try {
      dispatcher.invoke("wrong arg");
    } catch (Exception e) {
      assertThat(e.getClass(), is(IllegalArgumentException.class));
    }
  }

  @Test public void testCreateMethodDispatcherAndInvokeWrongArgument() {
    ReflectUtil.MethodDispatcher<Result> dispatcher = getResultMethodDispatcher();
    try {
      dispatcher.invoke("wrong arg");
    } catch (Exception e) {
      assertThat(e.getClass(), is(IllegalArgumentException.class));
    }
  }

  @Test public void testCreateMethodDispatcherAndInvokeNotImplement() {
    ReflectUtil.MethodDispatcher<Result> dispatcher = getResultMethodDispatcher();
    try {
      dispatcher.invoke(new DoubleDelta(100.0));
    } catch (Exception e) {
      assertThat(e.getClass(), is(IllegalArgumentException.class));
    }
  }

  private ReflectUtil.MethodDispatcher<Result> getResultMethodDispatcher() {
    ReflectiveVisitor visitor = new Meter(10);
    return ReflectUtil.createMethodDispatcher(Result.class, visitor, "increment",
        Delta.class);
  }

  private void checkGetBoxingClass(PrimitiveType type) {
    Class clazz = ReflectUtil.getBoxingClass(type.value());
    assertThat(clazz, is(type.wrappedClass()));
  }

  private void checkGetByteBufferReadAndWriteMethod(PrimitiveType type) {
    Method methodGet = ReflectUtil.getByteBufferReadMethod(type.value());
    assertThat(
        methodGet.toString(), is("public abstract " + type.valueClassSimpleName()
        + " java.nio.ByteBuffer.get"
        + (type == PrimitiveType.BYTE ? ""
            : StringUtils.capitalize(type.valueClassSimpleName()))
        + "(int)"));

    Method methodPut = ReflectUtil.getByteBufferWriteMethod(type.value());
    assertThat(
        methodPut.toString(), is("public abstract java.nio.ByteBuffer java.nio.ByteBuffer.put"
        + (type == PrimitiveType.BYTE ? ""
            : StringUtils.capitalize(type.valueClassSimpleName()))
        + "(int," + type.valueClassSimpleName() + ")"));
  }

  enum PrimitiveType {

    BYTE {
      @Override public Class<?> value() {
        return byte.class;
      }

      @Override public Class<?> wrappedClass() {
        return Byte.class;
      }

      @Override public int byteSize() {
        return 1;
      }
    },
    CHAR {
      @Override public Class<?> value() {
        return char.class;
      }

      @Override public Class<?> wrappedClass() {
        return Character.class;
      }

      @Override public int byteSize() {
        return 2;
      }

    },
    SHORT {
      @Override public Class<?> value() {
        return short.class;
      }

      @Override public Class<?> wrappedClass() {
        return Short.class;
      }

      @Override public int byteSize() {
        return 2;
      }
    },
    INT {
      @Override public Class<?> value() {
        return int.class;
      }

      @Override public Class<?> wrappedClass() {
        return Integer.class;
      }

      @Override public int byteSize() {
        return 4;
      }
    },
    LONG {
      @Override public Class<?> value() {
        return long.class;
      }

      @Override public Class<?> wrappedClass() {
        return Long.class;
      }

      @Override public int byteSize() {
        return 8;
      }
    },
    FLOAT {
      @Override public Class<?> value() {
        return float.class;
      }

      @Override public Class<?> wrappedClass() {
        return Float.class;
      }

      @Override public int byteSize() {
        return 4;
      }
    },
    DOUBLE {
      @Override public Class<?> value() {
        return double.class;
      }

      @Override public Class<?> wrappedClass() {
        return Double.class;
      }

      @Override public int byteSize() {
        return 8;
      }
    };

    public abstract Class<?> value();

    public abstract Class<?> wrappedClass();

    public abstract int byteSize();

    public String valueClassSimpleName() {
      return value().getSimpleName();
    }
  }

  public Method getMethod(Class<?> clazz, String methodName, Class<?>... parameterTypes) {
    if (clazz == null || StringUtils.isEmpty(methodName)) {
      return null;
    }

    for (Class<?> itr = clazz; hasSuperClass(itr);) {
      Method[] methods = itr.getDeclaredMethods();

      for (Method method : methods) {
        if (method.getName().equals(methodName) && Arrays.equals(method.getParameterTypes(),
            parameterTypes)) {
          return method;
        }
      }

      itr = itr.getSuperclass();
    }

    return null;
  }

  boolean hasSuperClass(Class<?> clazz) {
    return (clazz != null) && !clazz.equals(Object.class);
  }

  class Car {
    void drive(String driver) {

    }
  }

  class Person {
    void say(@Parameter(name = "text", optional = true) String text, String text2) {

    }
  }

  class Result {
    long result;

    Result(long result) {
      this.result = result;
    }
  }

  abstract class Delta {
    abstract long getDelta();
  }

  class IntDelta extends Delta {
    int delta;

    IntDelta(int delta) {
      this.delta = delta;
    }

    @Override public long getDelta() {
      return delta;
    }
  }

  class LongDelta extends Delta {
    long delta;

    LongDelta(long delta) {
      this.delta = delta;
    }

    @Override public long getDelta() {
      return delta;
    }
  }

  class DoubleDelta extends Delta {
    double delta;

    DoubleDelta(double delta) {
      this.delta = delta;
    }

    @Override public long getDelta() {
      return (long) delta;
    }
  }

  class Meter implements ReflectiveVisitor {
    long value = 0;

    Meter(long value) {
      this.value = value;
    }

    public Result increment(IntDelta delta) {
      value += delta.delta;
      return new Result(value);
    }

    public Result increment(LongDelta delta) {
      value += delta.delta;
      return new Result(value);
    }
  }
}
