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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlAccessType;

import java.util.List;

/**
 * Supplies a {@link SqlValidator} with the metadata for a table.
 *
 * @see SqlValidatorCatalogReader
 */
public interface SqlValidatorTable {

  /**
   * Holds a RexBuilder. Insulates SqlValidator implementations from Rex dependencies.
   */
  class RexBuilderHolder {
    public final RexBuilder rexBuilder;

    RexBuilderHolder(RelDataTypeFactory typeFactory) {
      this.rexBuilder = new RexBuilder(typeFactory);
    }
  }

  //~ Methods ----------------------------------------------------------------

  RelDataType getRowType();

  List<String> getQualifiedName();

  /**
   * Returns whether a given column is monotonic.
   */
  SqlMonotonicity getMonotonicity(String columnName);

  /**
   * Returns the access type of the table
   */
  SqlAccessType getAllowedAccess();

  boolean supportsModality(SqlModality modality);

  /**
   * Returns whether the ordinal column has a default value.
   */
  boolean columnHasDefaultValue(RelDataType rowType, int ordinal,
      RexBuilderHolder rexBuilderHolder);

  /**
   * Finds an interface implemented by this table.
   */
  <T> T unwrap(Class<T> clazz);
}

// End SqlValidatorTable.java
