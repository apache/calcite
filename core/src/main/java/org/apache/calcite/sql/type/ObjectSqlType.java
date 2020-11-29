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

import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * ObjectSqlType represents an SQL structured user-defined type.
 */
public class ObjectSqlType extends AbstractSqlType {
  //~ Instance fields --------------------------------------------------------

  private final @Nullable SqlIdentifier sqlIdentifier;

  private final RelDataTypeComparability comparability;

  private @Nullable RelDataTypeFamily family;

  //~ Constructors -----------------------------------------------------------

  /**
   * Constructs an object type. This should only be called from a factory
   * method.
   *
   * @param typeName      SqlTypeName for this type (either Distinct or
   *                      Structured)
   * @param sqlIdentifier identifier for this type
   * @param nullable      whether type accepts nulls
   * @param fields        object attribute definitions
   */
  public ObjectSqlType(
      SqlTypeName typeName,
      @Nullable SqlIdentifier sqlIdentifier,
      boolean nullable,
      List<? extends RelDataTypeField> fields,
      RelDataTypeComparability comparability) {
    super(typeName, nullable, fields);
    this.sqlIdentifier = sqlIdentifier;
    this.comparability = comparability;
    computeDigest();
  }

  //~ Methods ----------------------------------------------------------------

  public void setFamily(RelDataTypeFamily family) {
    this.family = family;
  }

  @Override public RelDataTypeComparability getComparability() {
    return comparability;
  }

  @Override public @Nullable SqlIdentifier getSqlIdentifier() {
    return sqlIdentifier;
  }

  @Override public RelDataTypeFamily getFamily() {
    // each UDT is in its own lonely family, until one day when
    // we support inheritance (at which time also need to implement
    // getPrecedenceList).
    RelDataTypeFamily family = this.family;
    return family != null ? family : this;
  }

  @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    // TODO jvs 10-Feb-2005:  proper quoting; dump attributes withDetail?
    sb.append("ObjectSqlType(");
    sb.append(sqlIdentifier);
    sb.append(")");
  }
}
