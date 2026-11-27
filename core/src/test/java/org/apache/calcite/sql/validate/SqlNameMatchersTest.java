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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests indexed matching and custom name-matching extension points. */
class SqlNameMatchersTest {
  @Test void testCustomMatchingAndFrequencyArePreserved() {
    final RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder builder = factory.builder();
    builder.add("a_b", SqlTypeName.INTEGER).add("ab", SqlTypeName.INTEGER);
    for (int i = 0; i < 32; i++) {
      builder.add("c" + i, SqlTypeName.INTEGER);
    }
    final RelDataType row = builder.build();
    final AtomicInteger frequencyCalls = new AtomicInteger();
    final SqlNameMatcher matcher = new SqlNameMatcher() {
      @Override public boolean isCaseSensitive() {
        return true;
      }

      @Override public boolean matches(String string, String name) {
        return string.replace("_", "").equals(name.replace("_", ""));
      }

      @Override public <K extends List<String>, V> V get(Map<K, V> map,
          List<String> prefixNames, List<String> names) {
        throw new UnsupportedOperationException();
      }

      @Override public String bestString() {
        throw new UnsupportedOperationException();
      }

      @Override public RelDataTypeField field(RelDataType rowType, String fieldName) {
        throw new UnsupportedOperationException();
      }

      @Override public int frequency(Iterable<String> names, String name) {
        frequencyCalls.incrementAndGet();
        int count = 0;
        for (String candidate : names) {
          if (matches(candidate, name)) {
            count++;
          }
        }
        return count;
      }

      @Override public Set<String> createSet() {
        throw new UnsupportedOperationException();
      }
    };
    assertEquals(row.getFieldList().subList(0, 2), matcher.fields(row, "ab"));
    assertEquals(2, matcher.fieldFrequency(row, "ab"));
    assertEquals(1, frequencyCalls.get());
  }

  @Test void testBuiltinMatchingCountsAllCaseCollisions() {
    final RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder builder = factory.builder();
    builder.add("A", SqlTypeName.INTEGER).add("a", SqlTypeName.INTEGER);
    for (int i = 0; i < 32; i++) {
      builder.add("c" + i, SqlTypeName.INTEGER);
    }
    final RelDataType row = builder.build();
    for (SqlNameMatcher matcher : Arrays.asList(SqlNameMatchers.withCaseSensitive(true),
        SqlNameMatchers.withCaseSensitive(false), SqlNameMatchers.liberal())) {
      assertEquals(matcher.frequency(row.getFieldNames(), "a"),
          matcher.fieldFrequency(row, "a"));
    }
  }
}
