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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;

import com.google.common.collect.ImmutableList;

/** Type that applies generic type to type parameters. */
abstract class ApplySqlType extends AbstractSqlType {
  protected final ImmutableList<? extends RelDataType> types;

  ApplySqlType(SqlTypeName typeName, boolean isNullable,
      Iterable<? extends RelDataType> types) {
    super(typeName, isNullable, null);
    this.types = ImmutableList.copyOf(types);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * {@inheritDoc}
   *
   * <p>Generate, for example, {@code MEASURE<INTEGER>}.
   */
  @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append(typeName)
        .append('<');
    Ord.forEach(types, (type, i) -> {
      if (i > 0) {
        sb.append(", ");
      }
      if (withDetail) {
        sb.append(type.getFullTypeString());
      } else {
        sb.append(type.toString());
      }
    });
    sb.append('>');
  }
}
