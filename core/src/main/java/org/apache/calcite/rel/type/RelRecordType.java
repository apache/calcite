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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * RelRecordType represents a structured type having named fields.
 */
public class RelRecordType extends RelDataTypeImpl implements Serializable {
  /** Name resolution policy; usually {@link StructKind#FULLY_QUALIFIED}. */
  private final StructKind kind;
  private final boolean nullable;

  /** First dynamic star on a wide record, or -1. */
  private final int fallbackFieldIndex;

  /** Candidate indexes are derived from the fields and rebuilt on demand. */
  private transient volatile @Nullable Map<String, List<RelDataTypeField>> fieldNameMap;
  private transient volatile @Nullable Map<CaseInsensitiveFieldName, List<RelDataTypeField>>
      insensitiveFieldNameMap;

  /** Minimum number of fields where it is worth populating {@link #fieldNameMap}
   * to accelerate lookups by field name. */
  private static final int THRESHOLD = 20;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>RecordType</code>. This should only be called from a
   * factory method.
   *
   * @param kind Name resolution policy
   * @param fields List of fields
   * @param nullable Whether this record type allows null values
   */
  public RelRecordType(StructKind kind, List<RelDataTypeField> fields, boolean nullable) {
    super(fields);
    this.nullable = nullable;
    this.kind = requireNonNull(kind, "kind");
    int fallback = -1;
    if (fields.size() > THRESHOLD) {
      for (Ord<RelDataTypeField> field : Ord.zip(requireNonNull(fieldList, "fieldList"))) {
        if (field.e.isDynamicStar()) {
          fallback = field.i;
          break;
        }
      }
    }
    this.fallbackFieldIndex = fallback;
    computeDigest();
  }

  /**
   * Creates a <code>RecordType</code>. This should only be called from a
   * factory method.
   * Shorthand for <code>RelRecordType(kind, fields, false)</code>.
   */
  public RelRecordType(StructKind kind, List<RelDataTypeField> fields) {
    this(kind, fields, false);
  }

  /**
   * Creates a <code>RecordType</code>. This should only be called from a
   * factory method.
   * Shorthand for <code>RelRecordType(StructKind.FULLY_QUALIFIED, fields, false)</code>.
   */
  public RelRecordType(List<RelDataTypeField> fields) {
    this(StructKind.FULLY_QUALIFIED, fields, false);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlTypeName getSqlTypeName() {
    return SqlTypeName.ROW;
  }

  @Override public boolean isNullable() {
    return nullable;
  }

  @Override public int getPrecision() {
    // REVIEW: angel 18-Aug-2005 Put in fake implementation for precision
    return 0;
  }

  @Override public StructKind getStructKind() {
    return kind;
  }

  @Override public @Nullable RelDataTypeField getField(String fieldName,
      boolean caseSensitive, boolean elideRecord) {
    final List<RelDataTypeField> matches = indexedFields(fieldName, caseSensitive);
    if (matches == null) {
      return super.getField(fieldName, caseSensitive, elideRecord);
    }
    if (!matches.isEmpty()) {
      return matches.get(0);
    }
    return resolveFieldFallback(fieldName, caseSensitive, elideRecord);
  }

  @Override public List<RelDataTypeField> getFields(String fieldName, boolean caseSensitive) {
    final List<RelDataTypeField> matches = indexedFields(fieldName, caseSensitive);
    return matches != null ? matches : super.getFields(fieldName, caseSensitive);
  }

  @Override protected @Nullable RelDataTypeField getFieldFallback() {
    if (fieldList != null && fieldList.size() > THRESHOLD) {
      return fallbackFieldIndex < 0 ? null : fieldList.get(fallbackFieldIndex);
    }
    return super.getFieldFallback();
  }

  private @Nullable List<RelDataTypeField> indexedFields(String fieldName,
      boolean caseSensitive) {
    if (fieldList == null || fieldList.size() <= THRESHOLD) {
      return null;
    }
    final Map<?, List<RelDataTypeField>> index = getFieldIndex(caseSensitive);
    final Object key = caseSensitive ? fieldName : new CaseInsensitiveFieldName(fieldName);
    return index.getOrDefault(key, ImmutableList.of());
  }

  private Map<?, List<RelDataTypeField>> getFieldIndex(boolean caseSensitive) {
    Map<?, List<RelDataTypeField>> index =
        caseSensitive ? fieldNameMap : insensitiveFieldNameMap;
    if (index == null) {
      synchronized (this) {
        index = caseSensitive ? fieldNameMap : insensitiveFieldNameMap;
        if (index == null) {
          if (caseSensitive) {
            fieldNameMap = createFieldIndex(Function.identity());
            index = fieldNameMap;
          } else {
            insensitiveFieldNameMap = createFieldIndex(CaseInsensitiveFieldName::new);
            index = insensitiveFieldNameMap;
          }
        }
      }
    }
    return index;
  }

  private <K> Map<K, List<RelDataTypeField>> createFieldIndex(Function<String, K> keyFactory) {
    final Map<K, List<RelDataTypeField>> entries = new HashMap<>();
    for (RelDataTypeField field : requireNonNull(fieldList, "fieldList")) {
      final K key = keyFactory.apply(field.getName());
      entries.computeIfAbsent(key, k -> new ArrayList<>()).add(field);
    }
    entries.replaceAll((key, fields) -> ImmutableList.copyOf(fields));
    return Collections.unmodifiableMap(entries);
  }

  @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("RecordType");
    switch (kind) {
    case PEEK_FIELDS:
      sb.append(":peek");
      break;
    case PEEK_FIELDS_DEFAULT:
      sb.append(":peek_default");
      break;
    case PEEK_FIELDS_NO_EXPAND:
      sb.append(":peek_no_expand");
      break;
    default:
      break;
    }
    sb.append("(");
    for (Ord<RelDataTypeField> ord : Ord.zip(requireNonNull(fieldList, "fieldList"))) {
      if (ord.i > 0) {
        sb.append(", ");
      }
      RelDataTypeField field = ord.e;
      if (withDetail) {
        sb.append(field.getType().getFullTypeString());
      } else {
        sb.append(field.getType().toString());
      }
      sb.append(" ");
      sb.append(field.getName());
    }
    sb.append(")");
  }

  @Override public boolean deepEquals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || this.getClass() != obj.getClass()) {
      return false;
    }

    RelRecordType that = (RelRecordType) obj;
    if (kind != that.kind || nullable != that.nullable) {
      return false;
    }

    if (fieldList == null || that.fieldList == null) {
      return fieldList == null && that.fieldList == null;
    }

    if (fieldList.size() != that.fieldList.size()) {
      return false;
    }

    for (int i = 0; i < fieldList.size(); i++) {
      if (!fieldList.get(i).equals(that.fieldList.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override public int deepHashCode() {
    return Objects.hash(kind.ordinal(), nullable, fieldList);
  }

  /**
   * Per {@link Serializable} API, provides a replacement object to be written
   * during serialization.
   *
   * <p>This implementation converts this RelRecordType into a
   * SerializableRelRecordType, whose <code>readResolve</code> method converts
   * it back to a RelRecordType during deserialization.
   */
  private Object writeReplace() {
    return new SerializableRelRecordType(requireNonNull(fieldList, "fieldList"));
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Hash key that delegates matching to String.equalsIgnoreCase on this JDK.
   * Code-point folding only supplies a hash; it never decides equality. */
  private static final class CaseInsensitiveFieldName
      implements Comparable<CaseInsensitiveFieldName> {
    private final String name;
    private final int hash;

    private CaseInsensitiveFieldName(String name) {
      this.name = name;
      int h = name.length();
      for (int offset = 0; offset < name.length();) {
        final int codePoint = name.codePointAt(offset);
        h = 31 * h + Character.toLowerCase(Character.toUpperCase(codePoint));
        offset += Character.charCount(codePoint);
      }
      this.hash = h;
    }

    @Override public boolean equals(@Nullable Object obj) {
      return this == obj
          || obj instanceof CaseInsensitiveFieldName
          && name.equalsIgnoreCase(((CaseInsensitiveFieldName) obj).name);
    }

    @Override public int hashCode() {
      return hash;
    }

    @Override public int compareTo(CaseInsensitiveFieldName other) {
      final int c = Integer.compare(name.length(), other.name.length());
      return c != 0 ? c : name.compareToIgnoreCase(other.name);
    }
  }

  /**
   * Skinny object which has the same information content as a
   * {@link RelRecordType} but skips redundant stuff like digest and the
   * immutable list.
   */
  private static class SerializableRelRecordType implements Serializable {
    private final List<RelDataTypeField> fields;

    private SerializableRelRecordType(List<RelDataTypeField> fields) {
      this.fields = requireNonNull(fields, "fields");
    }

    /**
     * Per {@link Serializable} API. See
     * {@link RelRecordType#writeReplace()}.
     */
    private Object readResolve() {
      return new RelRecordType(fields);
    }
  }
}
