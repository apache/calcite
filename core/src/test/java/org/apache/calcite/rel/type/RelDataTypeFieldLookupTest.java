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
package org.apache.calcite.rel.type;

import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests field lookup and ambiguity candidates without repeatedly scanning a row. */
class RelDataTypeFieldLookupTest {
  private final RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  private final RelDataType integer = factory.createSqlType(SqlTypeName.INTEGER);

  @Test void testMatchingFieldsRetainDuplicatesAndOrder() {
    for (int width : new int[] {3, 20, 21, 64}) {
      final List<RelDataTypeField> fields = new ArrayList<>();
      fields.add(new RelDataTypeFieldImpl("A", 0, integer));
      fields.add(new RelDataTypeFieldImpl("a", 1, integer));
      fields.add(new RelDataTypeFieldImpl("a", 2, integer));
      while (fields.size() < width) {
        fields.add(new RelDataTypeFieldImpl("c" + fields.size(), fields.size(), integer));
      }
      final RelRecordType row = new RelRecordType(fields);
      assertEquals(fields.subList(0, 3), row.getFields("a", false));
      assertEquals(fields.subList(1, 3), row.getFields("a", true));
      assertEquals(fields.subList(0, 1), row.getFields("A", true));
      assertTrue(row.getFields("missing", false).isEmpty());
      assertSame(fields.get(0), row.getField("a", false, false));
      assertSame(fields.get(1), row.getField("a", true, false));
      assertNull(row.getField("missing", false, false));
      assertThrows(UnsupportedOperationException.class,
          () -> row.getFields("a", false).clear());
    }
  }

  @Test void testWideCaseSensitiveLookupsDoNotRescanFieldNames() {
    for (int width : new int[] {32, 64, 128, 1024}) {
      final AtomicInteger nameReads = new AtomicInteger();
      final List<RelDataTypeField> fields = new ArrayList<>();
      for (int i = 0; i < width; i++) {
        fields.add(new CountingField("c" + i, i, integer, nameReads));
      }
      final RelRecordType row = new RelRecordType(fields);
      nameReads.set(0);
      final SqlNameMatcher sensitive = SqlNameMatchers.withCaseSensitive(true);
      assertEquals(1, sensitive.fieldFrequency(row, "c0"));
      assertEquals(width, nameReads.get(), "The exact matching policy builds one index");
      final int indexedReads = nameReads.get();
      for (int repeat = 0; repeat < 6; repeat++) {
        for (int i = 0; i < width; i++) {
          assertSame(fields.get(i), row.getField("c" + i, true, false));
          assertSame(fields.get(i), sensitive.fields(row, "c" + i).get(0));
          assertEquals(1, sensitive.fieldFrequency(row, "c" + i));
        }
      }
      assertEquals(indexedReads, nameReads.get(), "Indexed lookups must not inspect field names");
      for (int i = 0; i < width; i++) {
        assertNull(row.getField("missing" + i, true, false));
        assertTrue(sensitive.fields(row, "missing" + i).isEmpty());
        assertEquals(0, sensitive.fieldFrequency(row, "missing" + i));
      }
      // The existing fallback inspects the last field for an _extra column.
      assertEquals(indexedReads + width, nameReads.get(), "Missing names must not scan the row");
    }
  }

  @Test void testMutableDynamicFieldsAreNotCached() {
    final DynamicRecordTypeImpl row = new DynamicRecordTypeImpl(factory);
    final SqlNameMatcher matcher = SqlNameMatchers.withCaseSensitive(false);
    for (int i = 0; i < 32; i++) {
      row.getField("c" + i, false, false);
    }
    assertTrue(matcher.fields(row, "new_column").isEmpty());
    final RelDataTypeField added = row.getField("new_column", false, false);
    assertEquals(Arrays.asList(added), matcher.fields(row, "NEW_COLUMN"));
  }

  @Test void testFirstCaseSensitiveLookupBuildsIndex() {
    final AtomicInteger nameReads = new AtomicInteger();
    final List<RelDataTypeField> fields = new ArrayList<>();
    for (int i = 0; i < 32; i++) {
      fields.add(new CountingField("c" + i, i, integer, nameReads));
    }
    final RelRecordType row = new RelRecordType(fields);
    nameReads.set(0);
    assertSame(fields.get(0), row.getField("c0", true, false));
    assertEquals(fields.size(), nameReads.get(), "First lookup should build the exact index");
    nameReads.set(0);
    assertEquals(fields.subList(0, 1), row.getFields("c0", true));
    assertSame(fields.get(0), row.getField("c0", true, false));
    assertEquals(0, nameReads.get(), "First-match lookups should reuse the exact index");
  }

  @Test void testEmptyNameAndDynamicStarLookupOrder() {
    final RelDataType starType = factory.createSqlType(SqlTypeName.DYNAMIC_STAR);
    for (int width : new int[] {3, 20, 21, 64}) {
      for (boolean starFirst : new boolean[] {true, false}) {
        final List<RelDataTypeField> fields = new ArrayList<>();
        final RelDataTypeField empty = new RelDataTypeFieldImpl("", starFirst ? 1 : 0, integer);
        final RelDataTypeField star = new RelDataTypeFieldImpl("**", starFirst ? 0 : 1, starType);
        fields.add(starFirst ? star : empty);
        fields.add(starFirst ? empty : star);
        while (fields.size() < width) {
          fields.add(new RelDataTypeFieldImpl("c" + fields.size(), fields.size(), integer));
        }
        final RelRecordType row = new RelRecordType(fields);
        for (boolean sensitive : new boolean[] {true, false}) {
          assertEquals(Arrays.asList(empty), row.getFields("", sensitive));
          assertSame(empty, row.getField("", sensitive, false));
          assertSame(star, row.getField("**", sensitive, false));
          assertSame(star, row.getField("missing", sensitive, false));
        }
      }
    }
  }

  @Test void testEmptyNameDoesNotSupplyDynamicFallback() {
    for (int width : new int[] {3, 20, 21, 64}) {
      final List<RelDataTypeField> fields = new ArrayList<>();
      final RelDataTypeField empty = new RelDataTypeFieldImpl("", 0, integer);
      fields.add(empty);
      while (fields.size() < width) {
        fields.add(new RelDataTypeFieldImpl("c" + fields.size(), fields.size(), integer));
      }
      final RelRecordType row = new RelRecordType(fields);
      for (boolean sensitive : new boolean[] {true, false}) {
        assertSame(empty, row.getField("", sensitive, false));
        assertNull(row.getField("missing", sensitive, false));
        assertTrue(row.getFields("missing", sensitive).isEmpty());
      }
    }
  }

  @SuppressWarnings("deprecation")
  @Test void testLookupsDoNotCallDeprecatedFieldMap() {
    for (int width : new int[] {3, 20, 21, 64}) {
      final List<RelDataTypeField> fields = new ArrayList<>();
      fields.add(new RelDataTypeFieldImpl("A", 0, integer));
      fields.add(new RelDataTypeFieldImpl("a", 1, integer));
      while (fields.size() < width) {
        fields.add(new RelDataTypeFieldImpl("c" + fields.size(), fields.size(), integer));
      }
      final RelRecordType row = new RelRecordType(fields) {
        @Deprecated
        @Override protected Map<String, RelDataTypeField> getFieldMap() {
          throw new AssertionError("Field lookup must not consult the deprecated map");
        }
      };
      for (boolean sensitive : new boolean[] {true, false}) {
        final List<RelDataTypeField> expected = scan(fields, "A", sensitive);
        assertEquals(expected, row.getFields("A", sensitive));
        assertSame(expected.get(0), row.getField("A", sensitive, false));
        assertTrue(row.getFields("missing", sensitive).isEmpty());
        assertNull(row.getField("missing", sensitive, false));
      }
    }
  }

  @SuppressWarnings("deprecation")
  @Test void testNonRecordTypeUsesScanningFallback() {
    final AtomicInteger nameReads = new AtomicInteger();
    final List<RelDataTypeField> fields = new ArrayList<>();
    for (int i = 0; i < 32; i++) {
      fields.add(new CountingField("c" + i, i, integer, nameReads));
    }
    final RelDataTypeImpl row = new RelDataTypeImpl(fields) {
      @Override public SqlTypeName getSqlTypeName() {
        return SqlTypeName.ROW;
      }

      @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append("TestRow");
      }
    };
    assertNull(row.getFieldMap());
    final SqlNameMatcher matcher = SqlNameMatchers.withCaseSensitive(false);
    for (int repeat = 0; repeat < 2; repeat++) {
      nameReads.set(0);
      assertEquals(fields.subList(0, 1), matcher.fields(row, "C0"));
      assertEquals(fields.size(), nameReads.get());
    }
    nameReads.set(0);
    assertSame(fields.get(0), row.getField("C0", false, false));
    assertEquals(1, nameReads.get(), "First-match scans should stop at the first match");
    assertTrue(row.getFields("C0", true).isEmpty());
  }

  @Test void testConcurrentCaseSensitiveLookupsShareOneIndex() throws Exception {
    final AtomicInteger nameReads = new AtomicInteger();
    final List<RelDataTypeField> fields = new ArrayList<>();
    for (int i = 0; i < 64; i++) {
      fields.add(new CountingField("c" + i, i, integer, nameReads));
    }
    final RelRecordType row = new RelRecordType(fields);
    nameReads.set(0);
    final CountDownLatch start = new CountDownLatch(1);
    final ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      final List<Future<List<RelDataTypeField>>> results = new ArrayList<>();
      final Callable<List<RelDataTypeField>> lookup = () -> {
        start.await();
        return row.getFields("c0", true);
      };
      for (int i = 0; i < 4; i++) {
        results.add(executor.submit(lookup));
      }
      start.countDown();
      for (Future<List<RelDataTypeField>> result : results) {
        assertEquals(fields.subList(0, 1), result.get());
      }
      assertEquals(fields.size(), nameReads.get());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test void testNestedExtraAndDynamicFallbacks() {
    final List<RelDataTypeField> fields = new ArrayList<>();
    final RelDataType nested = factory.builder().add("inside", integer).build();
    fields.add(new RelDataTypeFieldImpl("nested", 0, nested));
    for (int i = 1; i < 24; i++) {
      fields.add(new RelDataTypeFieldImpl("c" + i, i, integer));
    }
    final RelRecordType row = new RelRecordType(fields);
    assertSame(nested.getFieldList().get(0), row.getField("INSIDE", false, true));
    assertTrue(row.getFields("INSIDE", false).isEmpty());

    fields.add(new RelDataTypeFieldImpl("_extra", fields.size(), integer));
    final RelRecordType extra = new RelRecordType(fields);
    final RelDataTypeField synthesized = extra.getField("missing", false, false);
    assertNotNull(synthesized);
    assertEquals("missing", synthesized.getName());
    assertEquals(-1, synthesized.getIndex());
    assertTrue(extra.getFields("missing", false).isEmpty());

    fields.remove(fields.size() - 1);
    final RelDataType starType = factory.createSqlType(SqlTypeName.DYNAMIC_STAR);
    final RelDataTypeField star = new RelDataTypeFieldImpl("**", fields.size(), starType);
    fields.add(star);
    final RelRecordType dynamic = new RelRecordType(fields);
    assertSame(star, dynamic.getField("missing", false, false));
    assertTrue(dynamic.getFields("missing", false).isEmpty());
    assertSame(star, dynamic.getField("", true, false));
    assertSame(star, dynamic.getField("missing", true, false));
    assertTrue(dynamic.getFields("", true).isEmpty());
    assertTrue(dynamic.getFields("", false).isEmpty());
  }

  @Test void testNestedAndExtraFieldsPrecedeDynamicFallback() {
    final List<RelDataTypeField> fields = new ArrayList<>();
    final RelDataType starType = factory.createSqlType(SqlTypeName.DYNAMIC_STAR);
    final RelDataTypeField star = new RelDataTypeFieldImpl("**", 0, starType);
    fields.add(star);
    final RelDataType nested = factory.builder().add("inside", integer).build();
    fields.add(new RelDataTypeFieldImpl("nested", 1, nested));
    while (fields.size() < 24) {
      fields.add(new RelDataTypeFieldImpl("c" + fields.size(), fields.size(), integer));
    }
    fields.add(new RelDataTypeFieldImpl("_extra", fields.size(), integer));
    final RelRecordType row = new RelRecordType(fields);
    for (boolean sensitive : new boolean[] {true, false}) {
      assertSame(nested.getFieldList().get(0),
          row.getField(sensitive ? "inside" : "INSIDE", sensitive, true));
      final RelDataTypeField extra = row.getField("missing", sensitive, false);
      assertNotNull(extra);
      assertEquals("missing", extra.getName());
      assertEquals(-1, extra.getIndex());
      assertSame(integer, extra.getType());
      assertTrue(row.getFields("missing", sensitive).isEmpty());
      final RelDataTypeField empty = row.getField("", sensitive, false);
      assertNotNull(empty);
      assertEquals("", empty.getName());
      assertEquals(-1, empty.getIndex());
      assertSame(integer, empty.getType());
    }
  }

  private static List<RelDataTypeField> scan(List<RelDataTypeField> fields,
      String query, boolean sensitive) {
    final List<RelDataTypeField> matches = new ArrayList<>();
    for (RelDataTypeField field : fields) {
      if (sensitive ? field.getName().equals(query)
          : field.getName().equalsIgnoreCase(query)) {
        matches.add(field);
      }
    }
    return matches;
  }

  /** Counts field accesses independently of the lookup implementation. */
  private static class CountingField extends RelDataTypeFieldImpl {
    private final AtomicInteger nameReads;

    CountingField(String name, int index, RelDataType type, AtomicInteger nameReads) {
      super(name, index, type);
      this.nameReads = nameReads;
    }

    @Override public String getName() {
      nameReads.incrementAndGet();
      return super.getName();
    }
  }
}
