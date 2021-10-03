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

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.test.schemata.hr.Employee;
import org.apache.calcite.test.schemata.hr.HrSchema;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.BitSet;
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
          new Date(0), "1", BigDecimal.ZERO),
      new EveryType(
          true, Byte.MAX_VALUE, Character.MAX_VALUE, Short.MAX_VALUE,
          Integer.MAX_VALUE, Long.MAX_VALUE, Float.MAX_VALUE,
          Double.MAX_VALUE,
          null, null, null, null, null, null, null, null,
          null, null, null, null, null, null),
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
    public final Boolean wrapperBoolean;
    public final Byte wrapperByte;
    public final Character wrapperCharacter;
    public final Short wrapperShort;
    public final Integer wrapperInteger;
    public final Long wrapperLong;
    public final Float wrapperFloat;
    public final Double wrapperDouble;
    public final java.sql.Date sqlDate;
    public final Time sqlTime;
    public final Timestamp sqlTimestamp;
    public final Date utilDate;
    public final String string;
    public final BigDecimal bigDecimal;

    public EveryType(
        boolean primitiveBoolean,
        byte primitiveByte,
        char primitiveChar,
        short primitiveShort,
        int primitiveInt,
        long primitiveLong,
        float primitiveFloat,
        double primitiveDouble,
        Boolean wrapperBoolean,
        Byte wrapperByte,
        Character wrapperCharacter,
        Short wrapperShort,
        Integer wrapperInteger,
        Long wrapperLong,
        Float wrapperFloat,
        Double wrapperDouble,
        java.sql.Date sqlDate,
        Time sqlTime,
        Timestamp sqlTimestamp,
        Date utilDate,
        String string,
        BigDecimal bigDecimal) {
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
    public final String value;

    public IntAndString(int id, String value) {
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
