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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * NamedArraySqlType represents a User defined array type.
 */
public class NamedArraySqlType extends ArraySqlType {
  private final List<String> typeName;

  public NamedArraySqlType(
      SqlTypeName sqlTypeName, RelDataType elementType, boolean isNullable,
      List<String> typeName, long maxCardinality) {
    super(sqlTypeName, elementType, isNullable, maxCardinality);
    this.typeName = ImmutableList.copyOf(typeName);
    computeDigest();
  }

  @Override public @Nullable SqlIdentifier getSqlIdentifier() {
    return new SqlIdentifier(typeName == null ? ImmutableList.of() : typeName, SqlParserPos.ZERO);
  }

  @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append(getSqlIdentifier());
    sb.append(":");
    super.generateTypeString(sb, withDetail);
  }
}
