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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a Pig relation that is created by Pig Latin <code>LOAD</code>
 * statement. Only the default load function is supported at this pint
 * (PigStorage()).
 * @see https://pig.apache.org/docs/r0.13.0/basic.html#load
 */
public class PigTable extends AbstractTable {

  private final String filePath;
  private final String[] fieldNames;
  private final byte[] fieldTypes;

  public PigTable(String filePath, String[] fieldNames, byte[] fieldTypes) {
    Preconditions.checkArgument(fieldNames.length == fieldTypes.length);
    this.filePath = filePath;
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final int columnCount = fieldNames.length;
    final List<Pair<String, RelDataType>> columnDesc = new ArrayList<>(columnCount);
    for (int i = 0; i < columnCount; i++) {
      final RelDataType relDataType = typeFactory
        .createSqlType(PigDataType.valueOf(fieldTypes[i]).getSqlType());
      columnDesc.add(Pair.of(fieldNames[i], relDataType));
    }
    return typeFactory.createStructType(columnDesc);
  }

  public String getFilePath() {
    return filePath;
  }
}
// End PigTable.java
