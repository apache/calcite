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
package net.hydromatic.optiq.impl.csv;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.*;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.util.Pair;

import au.com.bytecode.opencsv.CSVReader;

import java.io.*;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Table based on a CSV file.
 */
class CsvTable
    extends AbstractQueryable<Object[]>
    implements TranslatableTable<Object[]> {
  private final CsvSchema schema;
  private final String tableName;
  private final File file;
  private final RelDataType rowType;
  private final List<CsvFieldType> fieldTypes;

  public CsvTable(CsvSchema schema, String tableName, File file,
      RelDataType rowType, List<CsvFieldType> fieldTypes) {
    this.schema = schema;
    this.tableName = tableName;
    this.file = file;
    this.rowType = rowType;
    this.fieldTypes = fieldTypes;
    assert rowType != null;
    assert schema != null;
    assert tableName != null;
  }

  /** Creates a table based on a CSV file, deducing its column types by reading
   * the first line of the file. */
  public static CsvTable create(CsvSchema schema, File file, String tableName) {
    final List<CsvFieldType> fieldTypes = new ArrayList<CsvFieldType>();
    final RelDataType rowType =
        deduceRowType(schema.typeFactory, file, fieldTypes);
    return new CsvTable(schema, tableName, file, rowType, fieldTypes);
  }

  public String toString() {
    return "CsvTable {" + tableName + "}";
  }

  public QueryProvider getProvider() {
    return schema.getQueryProvider();
  }

  public DataContext getDataContext() {
    return schema;
  }

  public Type getElementType() {
    return Object[].class;
  }

  public RelDataType getRowType() {
    return rowType;
  }

  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  public Expression getExpression() {
    return Expressions.call(
        schema.getExpression(),
        "getTable",
        Expressions.<Expression>list()
            .append(Expressions.constant(tableName))
            .append(Expressions.constant(getElementType())));
  }

  public Iterator<Object[]> iterator() {
    return Linq4j.enumeratorIterator(enumerator());
  }

  public Enumerator<Object[]> enumerator() {
    return new CsvEnumerator(
        file, fieldTypes.toArray(new CsvFieldType[fieldTypes.size()]));
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable)
  {
    return new CsvTableScan(
        context.getCluster(),
        relOptTable,
        this,
        RelOptUtil.getFieldNameList(relOptTable.getRowType()));
  }

  /** Deduces the names and types of a table's columns by reading the first line
   * of a CSV file. */
  static RelDataType deduceRowType(JavaTypeFactory typeFactory, File file,
      List<CsvFieldType> fieldTypes) {
    final List<RelDataType> types = new ArrayList<RelDataType>();
    final List<String> names = new ArrayList<String>();
    CSVReader reader = null;
    try {
      reader = new CSVReader(new FileReader(file));
      final String[] strings = reader.readNext();
      for (String string : strings) {
        final String name;
        final CsvFieldType fieldType;
        final int colon = string.indexOf(':');
        if (colon >= 0) {
          name = string.substring(0, colon);
          String typeString = string.substring(colon + 1);
          fieldType = CsvFieldType.of(typeString);
        } else {
          name = string;
          fieldType = null;
        }
        final RelDataType type;
        if (fieldType == null) {
          type = typeFactory.createJavaType(String.class);
        } else {
          type = fieldType.toType(typeFactory);
        }
        names.add(name);
        types.add(type);
        fieldTypes.add(fieldType);
      }
    } catch (IOException e) {
      // ignore
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          // ignore
        }
      }
    }
    if (names.isEmpty()) {
      names.add("line");
      types.add(typeFactory.createJavaType(String.class));
    }
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  /** Enumerator that reads from a CSV file. */
  private static class CsvEnumerator implements Enumerator<Object[]> {
    private final CSVReader reader;
    private final CsvFieldType[] fieldTypes;
    private Object[] current;

    public CsvEnumerator(File file, CsvFieldType[] fieldTypes) {
      this.fieldTypes = fieldTypes;
      try {
        this.reader = new CSVReader(new FileReader(file));
        this.reader.readNext(); // skip header row
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public Object[] current() {
      return current;
    }

    public boolean moveNext() {
      try {
        final String[] strings = reader.readNext();
        if (strings == null) {
          current = null;
          reader.close();
          return false;
        }
        current = convertRow(strings);
        return true;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private Object[] convertRow(String[] strings) {
      final Object[] objects = new Object[fieldTypes.length];
      for (int i = 0; i < fieldTypes.length; i++) {
        objects[i] = convert(fieldTypes[i], strings[i]);
      }
      return objects;
    }

    private Object convert(CsvFieldType fieldType, String string) {
      if (fieldType == null) {
        return string;
      }
      switch (fieldType) {
      default:
      case STRING:
        return string;
      case BOOLEAN:
        if (string.length() == 0) {
          return null;
        }
        return Boolean.parseBoolean(string);
      case BYTE:
        if (string.length() == 0) {
          return null;
        }
        return Byte.parseByte(string);
      case SHORT:
        if (string.length() == 0) {
          return null;
        }
        return Short.parseShort(string);
      case INT:
        if (string.length() == 0) {
          return null;
        }
        return Integer.parseInt(string);
      case LONG:
        if (string.length() == 0) {
          return null;
        }
        return Long.parseLong(string);
      case FLOAT:
        if (string.length() == 0) {
          return null;
        }
        return Float.parseFloat(string);
      case DOUBLE:
        if (string.length() == 0) {
          return null;
        }
        return Double.parseDouble(string);
      }
    }

    public void reset() {
      throw new UnsupportedOperationException();
    }
  }
}

// End CsvTable.java
