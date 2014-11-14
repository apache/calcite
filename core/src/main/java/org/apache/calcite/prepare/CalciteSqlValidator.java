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
package net.hydromatic.optiq.prepare;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlInsert;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.validate.SqlConformance;
import org.eigenbase.sql.validate.SqlValidatorImpl;

/** Validator. */
class OptiqSqlValidator extends SqlValidatorImpl {
  public OptiqSqlValidator(
      SqlOperatorTable opTab,
      OptiqCatalogReader catalogReader,
      JavaTypeFactory typeFactory) {
    super(opTab, catalogReader, typeFactory, SqlConformance.DEFAULT);
  }

  @Override
  protected RelDataType getLogicalSourceRowType(
      RelDataType sourceRowType, SqlInsert insert) {
    return ((JavaTypeFactory) typeFactory).toSql(sourceRowType);
  }

  @Override
  protected RelDataType getLogicalTargetRowType(
      RelDataType targetRowType, SqlInsert insert) {
    return ((JavaTypeFactory) typeFactory).toSql(targetRowType);
  }
}

// End OptiqSqlValidator.java
