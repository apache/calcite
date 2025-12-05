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
package org.apache.calcite.test.schemata.catchall;

import org.apache.calcite.adapter.java.Array;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.test.schemata.hr.Employee;
import org.apache.calcite.test.schemata.hr.HrSchema;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * Object whose fields are relations. Called "catch-all" because it's OK
 * if tests add new fields.
 */
@SuppressWarnings("UnusedVariable")
public class CatchallSchema {
  public final Enumerable<Employee> enumerable =
      Linq4j.asEnumerable(
          Arrays.asList(new HrSchema().emps));

  public final List<Employee> list =
      Arrays.asList(new HrSchema().emps);

  public final BitSet bitSet = new BitSet(1);

  @SuppressWarnings("JavaUtilDate")
  public final EveryType[] everyTypes = {
      new EveryType(
          false, (byte) 0, (char) 0, (short) 0, 0, 0L, 0F, 0D,
          false, (byte) 0, (char) 0, (short) 0, 0, 0L, 0F, 0D,
          new java.sql.Date(0), new Time(0), new Timestamp(0),
          new Date(0), "1", BigDecimal.ZERO, Collections.emptyList()),
      new EveryType(
          true, Byte.MAX_VALUE, Character.MAX_VALUE, Short.MAX_VALUE,
          Integer.MAX_VALUE, Long.MAX_VALUE, Float.MAX_VALUE,
          Double.MAX_VALUE,
          null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null),
  };

  public final AllPrivate[] allPrivates =
      {new AllPrivate()};

  public final BadType[] badTypes = {new BadType()};

  public final Employee[] prefixEmps = {
      new Employee(1, 10, "A", 0f, null),
      new Employee(2, 10, "Ab", 0f, null),
      new Employee(3, 10, "Abc", 0f, null),
      new Employee(4, 10, "Abd", 0f, null),
  };

  public final Integer[] primesBoxed = {1, 3, 5};

  public final int[] primes = {1, 3, 5};

  public final IntHolder[] primesCustomBoxed =
      {new IntHolder(1), new IntHolder(3),
          new IntHolder(5)};

  public final IntAndString[] nullables = {
      new IntAndString(1, "A"), new IntAndString(2,
      "B"), new IntAndString(2, "C"),
      new IntAndString(3, null)};

  public final IntAndString[] bools = {
      new IntAndString(1, "T"), new IntAndString(2,
      "F"), new IntAndString(3, null)};

  private static boolean isNumeric(Class type) {
    switch (Primitive.flavor(type)) {
    case BOX:
      return Primitive.ofBox(type).isNumeric();
    case PRIMITIVE:
      return Primitive.of(type).isNumeric();
    default:
      return Number.class.isAssignableFrom(type); // e.g. BigDecimal
    }
  }

  /** Record that has a field of every interesting type. */
  public static class EveryType {
    public final boolean primitiveBoolean;
    public final byte primitiveByte;
    public final char primitiveChar;
    public final short primitiveShort;
    public final int primitiveInt;
    public final long primitiveLong;
    public final float primitiveFloat;
    public final double primitiveDouble;
    public final @Nullable Boolean wrapperBoolean;
    public final @Nullable Byte wrapperByte;
    public final @Nullable Character wrapperCharacter;
    public final @Nullable Short wrapperShort;
    public final @Nullable Integer wrapperInteger;
    public final @Nullable Long wrapperLong;
    public final @Nullable Float wrapperFloat;
    public final @Nullable Double wrapperDouble;
    public final java.sql.@Nullable Date sqlDate;
    public final @Nullable Time sqlTime;
    public final @Nullable Timestamp sqlTimestamp;
    public final @Nullable Date utilDate;
    public final @Nullable String string;
    public final @Nullable BigDecimal bigDecimal;
    public final @Nullable @Array(component = String.class) List<String> list;

    public EveryType(
        boolean primitiveBoolean,
        byte primitiveByte,
        char primitiveChar,
        short primitiveShort,
        int primitiveInt,
        long primitiveLong,
        float primitiveFloat,
        double primitiveDouble,
        @Nullable Boolean wrapperBoolean,
        @Nullable Byte wrapperByte,
        @Nullable Character wrapperCharacter,
        @Nullable Short wrapperShort,
        @Nullable Integer wrapperInteger,
        @Nullable Long wrapperLong,
        @Nullable Float wrapperFloat,
        @Nullable Double wrapperDouble,
        java.sql.@Nullable Date sqlDate,
        @Nullable Time sqlTime,
        @Nullable Timestamp sqlTimestamp,
        @Nullable Date utilDate,
        @Nullable String string,
        @Nullable BigDecimal bigDecimal,
        @Nullable List<String> list) {
      this.primitiveBoolean = primitiveBoolean;
      this.primitiveByte = primitiveByte;
      this.primitiveChar = primitiveChar;
      this.primitiveShort = primitiveShort;
      this.primitiveInt = primitiveInt;
      this.primitiveLong = primitiveLong;
      this.primitiveFloat = primitiveFloat;
      this.primitiveDouble = primitiveDouble;
      this.wrapperBoolean = wrapperBoolean;
      this.wrapperByte = wrapperByte;
      this.wrapperCharacter = wrapperCharacter;
      this.wrapperShort = wrapperShort;
      this.wrapperInteger = wrapperInteger;
      this.wrapperLong = wrapperLong;
      this.wrapperFloat = wrapperFloat;
      this.wrapperDouble = wrapperDouble;
      this.sqlDate = sqlDate;
      this.sqlTime = sqlTime;
      this.sqlTimestamp = sqlTimestamp;
      this.utilDate = utilDate;
      this.string = string;
      this.bigDecimal = bigDecimal;
      this.list = list;
    }

    public static Enumerable<Field> fields() {
      return Linq4j.asEnumerable(EveryType.class.getFields());
    }

    public static Enumerable<Field> numericFields() {
      return fields()
          .where(v1 -> isNumeric(v1.getType()));
    }
  }

  /** All field are private, therefore the resulting record has no fields. */
  public static class AllPrivate {
    private final int x = 0;
  }

  /** Table that has a field that cannot be recognized as a SQL type. */
  public static class BadType {
    public final int integer = 0;
    public final BitSet bitSet = new BitSet(0);
  }

  /** Table that has integer and string fields. */
  public static class IntAndString {
    public final int id;
    public final @Nullable String value;

    public IntAndString(int id, @Nullable String value) {
      this.id = id;
      this.value = value;
    }
  }

  /**
   * Custom java class that holds just a single field.
   */
  public static class IntHolder {
    public final int value;

    public IntHolder(int value) {
      this.value = value;
    }
  }
}
