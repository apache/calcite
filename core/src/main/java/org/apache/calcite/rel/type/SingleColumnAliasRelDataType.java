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

import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.Charset;
import java.util.List;

/**
 * Specific type of RelDataType that corresponds to a single column table,
 * where column can have alias.
 *
 * <p>For instance:
 * <blockquote><pre>select rmp, rmp.i from table(ramp(3)) as rmp;</pre></blockquote>
 *
 * @see org.apache.calcite.sql.validate.AliasNamespace
 */
public class SingleColumnAliasRelDataType implements RelDataType {
  private final RelDataType original;
  private final RelDataType alias;

  public SingleColumnAliasRelDataType(RelDataType original, RelDataType alias) {
    assert original.isStruct() && original.getFieldCount() == 1;
    assert alias.isStruct() && alias.getFieldCount() == 1;
    this.original = original;
    this.alias = alias;
  }

  @Override public boolean isStruct() {
    return true;
  }

  @Override public List<RelDataTypeField> getFieldList() {
    return original.getFieldList();
  }

  @Override public List<String> getFieldNames() {
    return original.getFieldNames();
  }

  @Override public int getFieldCount() {
    return 1;
  }

  @Override public StructKind getStructKind() {
    return original.getStructKind();
  }

  @Override public @Nullable RelDataTypeField getField(final String fieldName,
      final boolean caseSensitive, final boolean elideRecord) {
    RelDataTypeField originalField = original.getField(fieldName, caseSensitive, elideRecord);
    return originalField == null
        ? alias.getField(fieldName, caseSensitive, elideRecord) : originalField;
  }

  @Override public boolean isNullable() {
    return original.isNullable();
  }

  @Override public @Nullable RelDataType getComponentType() {
    return original.getComponentType();
  }

  @Override public @Nullable RelDataType getKeyType() {
    return original.getKeyType();
  }

  @Override public @Nullable RelDataType getValueType() {
    return original.getValueType();
  }

  @Override public @Nullable Charset getCharset() {
    return original.getCharset();
  }

  @Override public @Nullable SqlCollation getCollation() {
    return original.getCollation();
  }

  @Override public @Nullable SqlIntervalQualifier getIntervalQualifier() {
    return original.getIntervalQualifier();
  }

  @Override public int getPrecision() {
    return original.getPrecision();
  }

  @Override public int getScale() {
    return original.getScale();
  }

  @Override public SqlTypeName getSqlTypeName() {
    return original.getSqlTypeName();
  }

  @Override public @Nullable SqlIdentifier getSqlIdentifier() {
    return original.getSqlIdentifier();
  }

  @Override public String getFullTypeString() {
    return original.getFullTypeString();
  }

  @Override public RelDataTypeFamily getFamily() {
    return original.getFamily();
  }

  @Override public RelDataTypePrecedenceList getPrecedenceList() {
    return original.getPrecedenceList();
  }

  @Override public RelDataTypeComparability getComparability() {
    return original.getComparability();
  }

  @Override public boolean isDynamicStruct() {
    return original.isDynamicStruct();
  }

  @Override public RelDataTypeDigest getDigest() {
    return original.getDigest();
  }

  @Override public boolean deepEquals(@Nullable Object obj) {
    return original.deepEquals(obj);
  }

  @Override public int deepHashCode() {
    return original.deepHashCode();
  }
}
