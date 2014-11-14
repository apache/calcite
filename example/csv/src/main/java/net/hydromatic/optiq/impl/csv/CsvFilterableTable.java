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
package net.hydromatic.optiq.impl.csv;

import net.hydromatic.linq4j.*;

import net.hydromatic.optiq.*;

import org.eigenbase.reltype.RelProtoDataType;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlKind;

import java.io.File;
import java.util.Iterator;
import java.util.List;

/**
 * Table based on a CSV file that can implement simple filtering.
 *
 * <p>It implements the {@link FilterableTable} interface, so Calcite gets
 * data by calling the {@link #scan(DataContext, List)} method.
 */
public class CsvFilterableTable extends CsvTable
    implements FilterableTable {
  /** Creates a CsvFilterableTable. */
  CsvFilterableTable(File file, RelProtoDataType protoRowType) {
    super(file, protoRowType);
  }

  public String toString() {
    return "CsvFilterableTable";
  }

  public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
    final String[] filterValues = new String[fieldTypes.size()];
    for (final Iterator<RexNode> i = filters.iterator(); i.hasNext();) {
      final RexNode filter = i.next();
      if (addFilter(filter, filterValues)) {
        i.remove();
      }
    }
    final int[] fields = CsvEnumerator.identityList(fieldTypes.size());
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new CsvEnumerator<Object[]>(file, filterValues,
            new CsvEnumerator.ArrayRowConverter(fieldTypes, fields));
      }
    };
  }

  private boolean addFilter(RexNode filter, Object[] filterValues) {
    if (filter.isA(SqlKind.EQUALS)) {
      final RexCall call = (RexCall) filter;
      RexNode left = call.getOperands().get(0);
      if (left.isA(SqlKind.CAST)) {
        left = ((RexCall) left).operands.get(0);
      }
      final RexNode right = call.getOperands().get(1);
      if (left instanceof RexInputRef
          && right instanceof RexLiteral) {
        final int index = ((RexInputRef) left).getIndex();
        if (filterValues[index] == null) {
          filterValues[index] = ((RexLiteral) right).getValue2().toString();
          return true;
        }
      }
    }
    return false;
  }
}

// End CsvFilterableTable.java
