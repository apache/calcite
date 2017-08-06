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
package org.apache.calcite.adapter.pig;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import org.apache.pig.data.DataType;

/**
 * Represents a Pig relation that is created by Pig Latin
 * <a href="https://pig.apache.org/docs/r0.13.0/basic.html#load">
 * <code>LOAD</code></a> statement.
 *
 * <p>Only the default load function is supported at this point (PigStorage()).
 *
 * <p>Only VARCHAR (CHARARRAY in Pig) type supported at this point.
 *
 * @see PigTableFactory
 */
public class PigTable extends AbstractTable implements TranslatableTable {

  private final String filePath;
  private final String[] fieldNames;

  /** Creates a PigTable. */
  public PigTable(String filePath, String[] fieldNames) {
    this.filePath = filePath;
    this.fieldNames = fieldNames;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (String fieldName : fieldNames) {
      // only supports CHARARRAY types for now
      final RelDataType relDataType = typeFactory
          .createSqlType(PigDataType.valueOf(DataType.CHARARRAY).getSqlType());
      final RelDataType nullableRelDataType = typeFactory
          .createTypeWithNullability(relDataType, true);
      builder.add(fieldName, nullableRelDataType);
    }
    return builder.build();
  }

  public String getFilePath() {
    return filePath;
  }

  @Override public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new PigTableScan(cluster, cluster.traitSetOf(PigRel.CONVENTION), relOptTable);
  }
}

// End PigTable.java
