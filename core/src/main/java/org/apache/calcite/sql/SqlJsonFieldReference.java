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
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SqlJsonFieldReference extends SqlNode {
  protected final String value;
  protected final ArrayList<SqlNode> subkeys;
  
  public SqlJsonFieldReference(String value, SqlParserPos pos) {
    super(pos);
    this.value = value;
    this.subkeys = new ArrayList<SqlNode>();
  }

  public void addSubkey(SqlNode subkey) {
    this.subkeys.add(subkey);
  }

  @Override public SqlJsonFieldReference clone(SqlParserPos pos) {
    return new SqlJsonFieldReference(value, pos);
  }

  @Override public SqlKind getKind() {
    return SqlKind.JSON_FIELD_REFERENCE;
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    // validator.validateLiteral(this);
  }

  @Override public <R> R accept(SqlVisitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override public boolean equalsDeep(@Nullable SqlNode node, Litmus litmus) {
    if (!(node instanceof SqlJsonFieldReference)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlJsonFieldReference that = (SqlJsonFieldReference) node;
    if (!this.equals(that)) {
      return litmus.fail("{} != {}", this, node);
    }
    return litmus.succeed();
  }

  @Override public SqlMonotonicity getMonotonicity(@Nullable SqlValidatorScope scope) {
    return SqlMonotonicity.CONSTANT;
  }

  @Override public boolean equals(@Nullable Object obj) {
    if (!(obj instanceof SqlJsonFieldReference)) {
      return false;
    }
    SqlJsonFieldReference that = (SqlJsonFieldReference) obj;
    return Objects.equals(value, that.value) && this.subkeys.equals(that.subkeys);
  }

  // TODO: fixme
  @Override public int hashCode() {
    return (value == null) ? 0 : value.hashCode();
  }
  @Override public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    writer.literal("\"" + String.valueOf(value) + "\"");
    for (int i=0; i < this.subkeys.size(); i++) {
      SqlNode subkey = this.subkeys.get(i);
      if (subkey instanceof SqlCharStringLiteral) {
        writer.keyword(".");
        String value = ((SqlCharStringLiteral)subkey).getValueAs(NlsString.class).getValue();
        writer.literal("\"" + value + "\"");
      } else {
        writer.keyword("[");
        subkey.unparse(writer, leftPrec, rightPrec);
        writer.keyword("]");
      }
    }
  }
}
