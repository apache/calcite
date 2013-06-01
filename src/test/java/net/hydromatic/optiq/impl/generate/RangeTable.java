/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.impl.generate;

import net.hydromatic.linq4j.Enumerator;

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.TableFactory;
import net.hydromatic.optiq.impl.AbstractTable;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeName;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Table that returns a range of integers.
 */
public class RangeTable extends AbstractTable<Integer> {
  private final int start;
  private final int end;

  protected RangeTable(
      Schema schema,
      Type elementType,
      RelDataType relDataType,
      String tableName,
      int start,
      int end) {
    super(schema, elementType, relDataType, tableName);
    this.start = start;
    this.end = end;
  }

  /** Creates a RangeTable. */
  public static RangeTable create(
      Schema schema, String tableName, String columnName, int start, int end) {
    final JavaTypeFactory typeFactory = schema.getTypeFactory();
    final RelDataType integerType =
        typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType rowType =
        typeFactory.createStructType(
            RelDataTypeFactory.FieldInfoBuilder
                .of(columnName, integerType));
    return new RangeTable(
        schema, Object[].class, rowType, tableName, start, end);
  }

  public Enumerator<Integer> enumerator() {
    return new Enumerator<Integer>() {
      int current = start - 1;

      public Integer current() {
        if (current >= end) {
          throw new NoSuchElementException();
        }
        return current;
      }

      public boolean moveNext() {
        ++current;
        return current < end;
      }

      public void reset() {
        current = start - 1;
      }
    };
  }

  /** Implementation of {@link net.hydromatic.optiq.TableFactory} that allows
   * a {@link RangeTable} to be included as a custom table in an Optiq model
   * file. */
  public static class Factory implements TableFactory<RangeTable> {
    public RangeTable create(
        Schema schema,
        String name,
        Map<String, Object> operand,
        RelDataType rowType) {
      final String columnName = (String) operand.get("column");
      final int start = (Integer) operand.get("start");
      final int end = (Integer) operand.get("end");
      return RangeTable.create(schema, name, columnName, start, end);
    }
  }
}

// End RangeTable.java
