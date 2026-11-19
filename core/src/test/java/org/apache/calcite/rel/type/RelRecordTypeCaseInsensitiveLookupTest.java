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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests indexed case-insensitive field lookup on wide record types. */
class RelRecordTypeCaseInsensitiveLookupTest {
  private final RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  private final RelDataType integer = factory.createSqlType(SqlTypeName.INTEGER);

  @Test void testUnicodeCaseMatchingAgreesWithScan() {
    final String[] names = {"I", "i", "İ", "ı", "Σ", "σ", "ς", "ß", "ẞ", "SS",
        "K", "K", "𐐀", "𐐨", "𐐀x", "𐐨X", "", "\uD800", "\uDC00",
        "\uD800A", "\uD800a", "\uDC00I", "\uDC00i"};
    final List<RelDataTypeField> fields = new ArrayList<>();
    for (String name : names) {
      fields.add(new RelDataTypeFieldImpl(name, fields.size(), integer));
    }
    while (fields.size() < 32) {
      fields.add(new RelDataTypeFieldImpl("padding" + fields.size(), fields.size(), integer));
    }
    final RelRecordType row = new RelRecordType(fields);
    for (boolean sensitive : new boolean[] {true, false}) {
      for (String query : names) {
        final List<RelDataTypeField> expected = scan(fields, query, sensitive);
        assertEquals(expected, row.getFields(query, sensitive), query);
        assertSame(expected.get(0), row.getField(query, sensitive, false), query);
      }
    }
  }

  @Test void testAllUnicodeCaseMappings() {
    for (int codePoint = 0; codePoint <= Character.MAX_CODE_POINT; codePoint++) {
      final int lower = Character.toLowerCase(codePoint);
      final int upper = Character.toUpperCase(codePoint);
      if (lower == codePoint && upper == codePoint) {
        continue;
      }
      final String original = String.valueOf(Character.toChars(codePoint));
      final String lowerName = String.valueOf(Character.toChars(lower));
      final String upperName = String.valueOf(Character.toChars(upper));
      final List<RelDataTypeField> fields = new ArrayList<>();
      for (String name : Arrays.asList(original, lowerName, upperName)) {
        fields.add(new RelDataTypeFieldImpl(name, fields.size(), integer));
      }
      while (fields.size() < 21) {
        fields.add(new RelDataTypeFieldImpl("padding" + fields.size(), fields.size(), integer));
      }
      final RelRecordType row = new RelRecordType(fields);
      for (String query : Arrays.asList(original, lowerName, upperName)) {
        assertEquals(scan(fields, query, false), row.getFields(query, false),
            () -> "Unicode field matching differs for " + original);
      }
    }
  }

  @Test void testSupplementaryCaseVariantsFollowRuntimeMatching() {
    final List<RelDataTypeField> fields = new ArrayList<>();
    for (int variant = 0; variant < 256; variant++) {
      final StringBuilder name = new StringBuilder();
      for (int bit = 0; bit < 8; bit++) {
        name.appendCodePoint((variant & (1 << bit)) == 0 ? 0x10400 : 0x10428);
      }
      fields.add(new RelDataTypeFieldImpl(name.toString(), variant, integer));
    }
    final RelRecordType row = new RelRecordType(fields);
    for (RelDataTypeField field : fields) {
      final String query = field.getName();
      final List<RelDataTypeField> expected = scan(fields, query, false);
      assertEquals(expected, row.getFields(query, false));
      assertSame(expected.get(0), row.getField(query, false, false));
      assertEquals(Arrays.asList(field), row.getFields(query, true));
    }
  }

  @Test void testWideLookupsDoNotRescanFieldNames() {
    for (int width : new int[] {32, 64, 128, 1024}) {
      final AtomicInteger nameReads = new AtomicInteger();
      final List<RelDataTypeField> fields = new ArrayList<>();
      for (int i = 0; i < width; i++) {
        fields.add(new CountingField("c" + i, i, integer, nameReads));
      }
      final RelRecordType row = new RelRecordType(fields);
      nameReads.set(0);
      final SqlNameMatcher insensitive = SqlNameMatchers.withCaseSensitive(false);
      final SqlNameMatcher sensitive = SqlNameMatchers.withCaseSensitive(true);
      final SqlNameMatcher liberal = SqlNameMatchers.liberal();
      assertEquals(1, insensitive.fieldFrequency(row, "C0"));
      assertEquals(1, sensitive.fieldFrequency(row, "c0"));
      assertEquals(2 * width, nameReads.get(), "Each matching policy builds one index");
      final int indexedReads = nameReads.get();
      for (int repeat = 0; repeat < 6; repeat++) {
        for (int i = 0; i < width; i++) {
          assertSame(fields.get(i), row.getField("C" + i, false, false));
          assertSame(fields.get(i), insensitive.fields(row, "C" + i).get(0));
          assertSame(fields.get(i), sensitive.fields(row, "c" + i).get(0));
          assertEquals(1, insensitive.fieldFrequency(row, "C" + i));
          assertEquals(1, sensitive.fieldFrequency(row, "c" + i));
          assertEquals(1, liberal.fieldFrequency(row, "C" + i));
        }
      }
      assertEquals(indexedReads, nameReads.get(), "Indexed lookups must not inspect field names");
      for (int i = 0; i < width; i++) {
        assertNull(row.getField("missing" + i, false, false));
        assertTrue(insensitive.fields(row, "missing" + i).isEmpty());
        assertEquals(0, insensitive.fieldFrequency(row, "missing" + i));
        assertEquals(0, sensitive.fieldFrequency(row, "missing" + i));
        assertEquals(0, liberal.fieldFrequency(row, "missing" + i));
      }
      // The existing fallback inspects the last field for an _extra column.
      assertEquals(indexedReads + width, nameReads.get(), "Missing names must not scan the row");
    }
  }

  @Test void testFirstLookupBuildsOnlyRequestedIndex() {
    for (boolean sensitive : new boolean[] {true, false}) {
      final AtomicInteger nameReads = new AtomicInteger();
      final List<RelDataTypeField> fields = new ArrayList<>();
      for (int i = 0; i < 32; i++) {
        fields.add(new CountingField("c" + i, i, integer, nameReads));
      }
      final RelRecordType row = new RelRecordType(fields);
      nameReads.set(0);
      assertSame(fields.get(0), row.getField(sensitive ? "c0" : "C0", sensitive, false));
      assertEquals(fields.size(), nameReads.get(), "Only the requested mode should be indexed");
      nameReads.set(0);
      assertEquals(fields.subList(0, 1), row.getFields(sensitive ? "c0" : "C0", sensitive));
      assertSame(fields.get(0), row.getField(sensitive ? "c0" : "C0", sensitive, false));
      assertEquals(0, nameReads.get(), "First-match lookups should reuse the requested index");
    }
  }

  @Test void testConcurrentLookupsShareOneIndex() throws Exception {
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
        return row.getFields("C0", false);
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
