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
package org.apache.calcite.jdbc;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlJdbcFunctionCall;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.util.Util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Field;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Helper for implementing the {@code getXxx} methods such as
 * {@link org.apache.calcite.avatica.AvaticaDatabaseMetaData#getTables}.
 */
public class CalciteMetaImpl extends MetaImpl {
  static final Driver DRIVER = new Driver();

  public CalciteMetaImpl(CalciteConnectionImpl connection) {
    super(connection);
  }

  static <T extends Named> Predicate1<T> namedMatcher(final Pat pattern) {
    if (pattern.s == null || pattern.s.equals("%")) {
      return Functions.truePredicate1();
    }
    final Pattern regex = likeToRegex(pattern);
    return new Predicate1<T>() {
      public boolean apply(T v1) {
        return regex.matcher(v1.getName()).matches();
      }
    };
  }

  static Predicate1<String> matcher(final Pat pattern) {
    if (pattern.s == null || pattern.s.equals("%")) {
      return Functions.truePredicate1();
    }
    final Pattern regex = likeToRegex(pattern);
    return new Predicate1<String>() {
      public boolean apply(String v1) {
        return regex.matcher(v1).matches();
      }
    };
  }

  /** Converts a LIKE-style pattern (where '%' represents a wild-card, escaped
   * using '\') to a Java regex. */
  public static Pattern likeToRegex(Pat pattern) {
    StringBuilder buf = new StringBuilder("^");
    char[] charArray = pattern.s.toCharArray();
    int slash = -2;
    for (int i = 0; i < charArray.length; i++) {
      char c = charArray[i];
      if (slash == i - 1) {
        buf.append('[').append(c).append(']');
      } else {
        switch (c) {
        case '\\':
          slash = i;
          break;
        case '%':
          buf.append(".*");
          break;
        case '[':
          buf.append("\\[");
          break;
        case ']':
          buf.append("\\]");
          break;
        default:
          buf.append('[').append(c).append(']');
        }
      }
    }
    buf.append("$");
    return Pattern.compile(buf.toString());
  }

  @Override public StatementHandle createStatement(ConnectionHandle ch) {
    final StatementHandle h = super.createStatement(ch);
    final CalciteConnectionImpl calciteConnection = getConnection();
    calciteConnection.server.addStatement(calciteConnection, h);
    return h;
  }

  @Override public void closeStatement(StatementHandle h) {
    final CalciteConnectionImpl calciteConnection = getConnection();
    CalciteServerStatement stmt = calciteConnection.server.getStatement(h);
    // stmt.close(); // TODO: implement
    calciteConnection.server.removeStatement(h);
  }

  private <E> MetaResultSet createResultSet(Enumerable<E> enumerable,
      Class clazz, String... names) {
    final List<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();
    final List<Field> fields = new ArrayList<Field>();
    final List<String> fieldNames = new ArrayList<String>();
    for (String name : names) {
      final int index = fields.size();
      final String fieldName = AvaticaUtils.toCamelCase(name);
      final Field field;
      try {
        field = clazz.getField(fieldName);
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
      columns.add(columnMetaData(name, index, field.getType()));
      fields.add(field);
      fieldNames.add(fieldName);
    }
    //noinspection unchecked
    final Iterable<Object> iterable = (Iterable<Object>) (Iterable) enumerable;
    return createResultSet(Collections.<String, Object>emptyMap(),
        columns, CursorFactory.record(clazz, fields, fieldNames),
        new Frame(0, true, iterable));
  }

  @Override protected <E> MetaResultSet
  createEmptyResultSet(final Class<E> clazz) {
    final List<ColumnMetaData> columns = fieldMetaData(clazz).columns;
    final CursorFactory cursorFactory = CursorFactory.deduce(columns, clazz);
    return createResultSet(Collections.<String, Object>emptyMap(), columns,
        cursorFactory, Frame.EMPTY);
  }

  protected MetaResultSet createResultSet(
      Map<String, Object> internalParameters, List<ColumnMetaData> columns,
      CursorFactory cursorFactory, final Frame firstFrame) {
    try {
      final CalciteConnectionImpl connection = getConnection();
      final AvaticaStatement statement = connection.createStatement();
      final CalcitePrepare.CalciteSignature<Object> signature =
          new CalcitePrepare.CalciteSignature<Object>("",
              ImmutableList.<AvaticaParameter>of(), internalParameters, null,
              columns, cursorFactory, -1, null) {
            @Override public Enumerable<Object> enumerable(
                DataContext dataContext) {
              return Linq4j.asEnumerable(firstFrame.rows);
            }
          };
      return new MetaResultSet(statement.getId(), true, signature, firstFrame);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  CalciteConnectionImpl getConnection() {
    return (CalciteConnectionImpl) connection;
  }

  public String getSqlKeywords() {
    return SqlParser.create("").getMetadata().getJdbcKeywords();
  }

  public String getNumericFunctions() {
    return SqlJdbcFunctionCall.getNumericFunctions();
  }

  public String getStringFunctions() {
    return SqlJdbcFunctionCall.getStringFunctions();
  }

  public String getSystemFunctions() {
    return SqlJdbcFunctionCall.getSystemFunctions();
  }

  public String getTimeDateFunctions() {
    return SqlJdbcFunctionCall.getTimeDateFunctions();
  }

  public MetaResultSet getTables(String catalog,
      final Pat schemaPattern,
      final Pat tableNamePattern,
      final List<String> typeList) {
    final Predicate1<MetaTable> typeFilter;
    if (typeList == null) {
      typeFilter = Functions.truePredicate1();
    } else {
      typeFilter = new Predicate1<MetaTable>() {
        public boolean apply(MetaTable v1) {
          return typeList.contains(v1.tableType);
        }
      };
    }
    final Predicate1<MetaSchema> schemaMatcher = namedMatcher(schemaPattern);
    return createResultSet(schemas(catalog)
            .where(schemaMatcher)
            .selectMany(
                new Function1<MetaSchema, Enumerable<MetaTable>>() {
                  public Enumerable<MetaTable> apply(MetaSchema schema) {
                    return tables(schema, matcher(tableNamePattern));
                  }
                })
            .where(typeFilter),
        MetaTable.class,
        "TABLE_CAT",
        "TABLE_SCHEM",
        "TABLE_NAME",
        "TABLE_TYPE",
        "REMARKS",
        "TYPE_CAT",
        "TYPE_SCHEM",
        "TYPE_NAME",
        "SELF_REFERENCING_COL_NAME",
        "REF_GENERATION");
  }

  public MetaResultSet getColumns(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern) {
    final Predicate1<String> tableNameMatcher = matcher(tableNamePattern);
    final Predicate1<MetaSchema> schemaMatcher = namedMatcher(schemaPattern);
    final Predicate1<MetaColumn> columnMatcher =
        namedMatcher(columnNamePattern);
    return createResultSet(schemas(catalog)
            .where(schemaMatcher)
            .selectMany(
                new Function1<MetaSchema, Enumerable<MetaTable>>() {
                  public Enumerable<MetaTable> apply(MetaSchema schema) {
                    return tables(schema, tableNameMatcher);
                  }
                })
            .selectMany(
                new Function1<MetaTable, Enumerable<MetaColumn>>() {
                  public Enumerable<MetaColumn> apply(MetaTable schema) {
                    return columns(schema);
                  }
                })
            .where(columnMatcher),
        MetaColumn.class,
        "TABLE_CAT",
        "TABLE_SCHEM",
        "TABLE_NAME",
        "COLUMN_NAME",
        "DATA_TYPE",
        "TYPE_NAME",
        "COLUMN_SIZE",
        "BUFFER_LENGTH",
        "DECIMAL_DIGITS",
        "NUM_PREC_RADIX",
        "NULLABLE",
        "REMARKS",
        "COLUMN_DEF",
        "SQL_DATA_TYPE",
        "SQL_DATETIME_SUB",
        "CHAR_OCTET_LENGTH",
        "ORDINAL_POSITION",
        "IS_NULLABLE",
        "SCOPE_CATALOG",
        "SCOPE_TABLE",
        "SOURCE_DATA_TYPE",
        "IS_AUTOINCREMENT",
        "IS_GENERATEDCOLUMN");
  }

  Enumerable<MetaCatalog> catalogs() {
    return Linq4j.asEnumerable(
        ImmutableList.of(new MetaCatalog(connection.getCatalog())));
  }

  Enumerable<MetaTableType> tableTypes() {
    return Linq4j.asEnumerable(
        ImmutableList.of(
            new MetaTableType("TABLE"), new MetaTableType("VIEW")));
  }

  Enumerable<MetaSchema> schemas(String catalog) {
    return Linq4j.asEnumerable(
        getConnection().rootSchema.getSubSchemaMap().values())
        .select(
            new Function1<CalciteSchema, MetaSchema>() {
              public MetaSchema apply(CalciteSchema calciteSchema) {
                return new CalciteMetaSchema(
                    calciteSchema,
                    connection.getCatalog(),
                    calciteSchema.getName());
              }
            })
        .orderBy(
            new Function1<MetaSchema, Comparable>() {
              public Comparable apply(MetaSchema metaSchema) {
                return (Comparable) FlatLists.of(
                    Util.first(metaSchema.tableCatalog, ""),
                    metaSchema.tableSchem);
              }
            });
  }

  Enumerable<MetaTable> tables(String catalog) {
    return schemas(catalog)
        .selectMany(
            new Function1<MetaSchema, Enumerable<MetaTable>>() {
              public Enumerable<MetaTable> apply(MetaSchema schema) {
                return tables(schema, Functions.<String>truePredicate1());
              }
            });
  }

  Enumerable<MetaTable> tables(final MetaSchema schema_) {
    final CalciteMetaSchema schema = (CalciteMetaSchema) schema_;
    return Linq4j.asEnumerable(schema.calciteSchema.getTableNames())
        .select(
            new Function1<String, MetaTable>() {
              public MetaTable apply(String name) {
                final Table table =
                    schema.calciteSchema.getTable(name, true).getValue();
                return new CalciteMetaTable(table,
                    schema.tableCatalog,
                    schema.tableSchem,
                    name);
              }
            })
        .concat(
            Linq4j.asEnumerable(
                schema.calciteSchema.getTablesBasedOnNullaryFunctions()
                    .entrySet())
                .select(
                    new Function1<Map.Entry<String, Table>, MetaTable>() {
                      public MetaTable apply(Map.Entry<String, Table> pair) {
                        final Table table = pair.getValue();
                        return new CalciteMetaTable(table,
                            schema.tableCatalog,
                            schema.tableSchem,
                            pair.getKey());
                      }
                    }));
  }

  Enumerable<MetaTable> tables(
      final MetaSchema schema,
      final Predicate1<String> matcher) {
    return tables(schema)
        .where(
            new Predicate1<MetaTable>() {
              public boolean apply(MetaTable v1) {
                return matcher.apply(v1.getName());
              }
            });
  }

  public Enumerable<MetaColumn> columns(final MetaTable table_) {
    final CalciteMetaTable table = (CalciteMetaTable) table_;
    final RelDataType rowType =
        table.calciteTable.getRowType(getConnection().typeFactory);
    return Linq4j.asEnumerable(rowType.getFieldList())
        .select(
            new Function1<RelDataTypeField, MetaColumn>() {
              public MetaColumn apply(RelDataTypeField field) {
                final int precision =
                    field.getType().getSqlTypeName().allowsPrec()
                        && !(field.getType()
                        instanceof RelDataTypeFactoryImpl.JavaType)
                        ? field.getType().getPrecision()
                        : -1;
                return new MetaColumn(
                    table.tableCat,
                    table.tableSchem,
                    table.tableName,
                    field.getName(),
                    field.getType().getSqlTypeName().getJdbcOrdinal(),
                    field.getType().getFullTypeString(),
                    precision,
                    field.getType().getSqlTypeName().allowsScale()
                        ? field.getType().getScale()
                        : null,
                    10,
                    field.getType().isNullable()
                        ? DatabaseMetaData.columnNullable
                        : DatabaseMetaData.columnNoNulls,
                    precision,
                    field.getIndex() + 1,
                    field.getType().isNullable() ? "YES" : "NO");
              }
            });
  }

  public MetaResultSet getSchemas(String catalog, Pat schemaPattern) {
    final Predicate1<MetaSchema> schemaMatcher = namedMatcher(schemaPattern);
    return createResultSet(schemas(catalog).where(schemaMatcher),
        MetaSchema.class,
        "TABLE_SCHEM",
        "TABLE_CATALOG");
  }

  public MetaResultSet getCatalogs() {
    return createResultSet(catalogs(),
        MetaCatalog.class,
        "TABLE_CATALOG");
  }

  public MetaResultSet getTableTypes() {
    return createResultSet(tableTypes(),
        MetaTableType.class,
        "TABLE_TYPE");
  }

  @Override public Iterable<Object> createIterable(StatementHandle handle,
      Signature signature, List<Object> parameterValues, Frame firstFrame) {
    try {
      //noinspection unchecked
      final CalcitePrepare.CalciteSignature<Object> calciteSignature =
          (CalcitePrepare.CalciteSignature<Object>) signature;
      return getConnection().enumerable(handle, calciteSignature);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public Signature prepare(StatementHandle h, String sql, int maxRowCount) {
    final CalciteConnectionImpl calciteConnection = getConnection();
    CalciteServerStatement statement = calciteConnection.server.getStatement(h);
    return calciteConnection.parseQuery(sql, statement.createPrepareContext(),
        maxRowCount);
  }

  public MetaResultSet prepareAndExecute(StatementHandle h, String sql,
      int maxRowCount, PrepareCallback callback) {
    final CalcitePrepare.CalciteSignature<Object> signature;
    try {
      synchronized (callback.getMonitor()) {
        callback.clear();
        final CalciteConnectionImpl calciteConnection = getConnection();
        CalciteServerStatement statement =
            calciteConnection.server.getStatement(h);
        signature = calciteConnection.parseQuery(sql,
            statement.createPrepareContext(), maxRowCount);
        callback.assign(signature, null);
      }
      callback.execute();
      return new MetaResultSet(h.id, false, signature, null);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    // TODO: share code with prepare and createIterable
  }

  /** A trojan-horse method, subject to change without notice. */
  @VisibleForTesting
  public static DataContext createDataContext(CalciteConnection connection) {
    return ((CalciteConnectionImpl) connection)
        .createDataContext(ImmutableMap.<String, Object>of());
  }

  /** A trojan-horse method, subject to change without notice. */
  @VisibleForTesting
  public static CalciteConnection connect(CalciteRootSchema schema,
      JavaTypeFactory typeFactory) {
    return DRIVER.connect(schema, typeFactory);
  }

  /** Metadata describing a Calcite table. */
  private static class CalciteMetaTable extends MetaTable {
    private final Table calciteTable;

    public CalciteMetaTable(Table calciteTable, String tableCat,
        String tableSchem, String tableName) {
      super(tableCat, tableSchem, tableName,
          calciteTable.getJdbcTableType().name());
      this.calciteTable = Preconditions.checkNotNull(calciteTable);
    }
  }

  /** Metadata describing a Calcite schema. */
  private static class CalciteMetaSchema extends MetaSchema {
    private final CalciteSchema calciteSchema;

    public CalciteMetaSchema(CalciteSchema calciteSchema,
        String tableCatalog, String tableSchem) {
      super(tableCatalog, tableSchem);
      this.calciteSchema = calciteSchema;
    }
  }

  /** Table whose contents are metadata. */
  abstract static class MetadataTable<E> extends AbstractQueryableTable {
    public MetadataTable(Class<E> clazz) {
      super(clazz);
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return ((JavaTypeFactory) typeFactory).createType(elementType);
    }

    @Override public Schema.TableType getJdbcTableType() {
      return Schema.TableType.SYSTEM_TABLE;
    }

    @SuppressWarnings("unchecked")
    @Override public Class<E> getElementType() {
      return (Class<E>) elementType;
    }

    protected abstract Enumerator<E> enumerator(CalciteMetaImpl connection);

    public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
        SchemaPlus schema, String tableName) {
      return new AbstractTableQueryable<T>(queryProvider, schema, this,
          tableName) {
        @SuppressWarnings("unchecked")
        public Enumerator<T> enumerator() {
          return (Enumerator<T>) MetadataTable.this.enumerator(
              ((CalciteConnectionImpl) queryProvider).meta());
        }
      };
    }
  }
}

// End CalciteMetaImpl.java
