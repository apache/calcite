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
package net.hydromatic.optiq.jdbc;

import net.hydromatic.avatica.*;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.linq4j.function.*;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.TableInSchemaImpl;
import net.hydromatic.optiq.impl.java.MapSchema;
import net.hydromatic.optiq.runtime.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.SqlJdbcFunctionCall;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.*;
import java.sql.Types;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Helper for implementing the {@code getXxx} methods such as
 * {@link net.hydromatic.avatica.AvaticaDatabaseMetaData#getTables}.
 */
public class MetaImpl implements Meta {
  private static final Map<Class, Pair<Integer, String>> MAP =
      ImmutableMap.<Class, Pair<Integer, String>>builder()
          .put(boolean.class, Pair.of(Types.BOOLEAN, "BOOLEAN"))
          .put(Boolean.class, Pair.of(Types.BOOLEAN, "BOOLEAN"))
          .put(byte.class, Pair.of(Types.TINYINT, "TINYINT"))
          .put(Byte.class, Pair.of(Types.TINYINT, "TINYINT"))
          .put(short.class, Pair.of(Types.SMALLINT, "SMALLINT"))
          .put(Short.class, Pair.of(Types.SMALLINT, "SMALLINT"))
          .put(int.class, Pair.of(Types.INTEGER, "INTEGER"))
          .put(Integer.class, Pair.of(Types.INTEGER, "INTEGER"))
          .put(long.class, Pair.of(Types.BIGINT, "BIGINT"))
          .put(Long.class, Pair.of(Types.BIGINT, "BIGINT"))
          .put(float.class, Pair.of(Types.FLOAT, "FLOAT"))
          .put(Float.class, Pair.of(Types.FLOAT, "FLOAT"))
          .put(double.class, Pair.of(Types.DOUBLE, "DOUBLE"))
          .put(Double.class, Pair.of(Types.DOUBLE, "DOUBLE"))
          .put(String.class, Pair.of(Types.VARCHAR, "VARCHAR"))
          .put(java.sql.Date.class, Pair.of(Types.DATE, "DATE"))
          .put(Time.class, Pair.of(Types.TIME, "TIME"))
          .put(Timestamp.class, Pair.of(Types.TIMESTAMP, "TIMESTAMP"))
          .build();

  final OptiqConnectionImpl connection;

  public MetaImpl(OptiqConnectionImpl connection) {
    this.connection = connection;
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

  static ColumnMetaData columnMetaData(String name, int index, Class<?> type) {
    Pair<Integer, String> pair = MAP.get(type);
    ColumnMetaData.Rep rep =
        ColumnMetaData.Rep.VALUE_MAP.get(type);
    return new ColumnMetaData(
        index, false, true, false, false,
        Primitive.is(type)
            ? DatabaseMetaData.columnNullable
            : DatabaseMetaData.columnNoNulls,
        true, -1, name, name, null,
        0, 0, null, null, pair.left, pair.right, true,
        false, false, null, rep);
  }

  static List<ColumnMetaData> fieldMetaData(Class clazz) {
    final List<ColumnMetaData> list = new ArrayList<ColumnMetaData>();
    for (Field field : clazz.getFields()) {
      if (Modifier.isPublic(field.getModifiers())
          && !Modifier.isStatic(field.getModifiers())) {
        list.add(
            columnMetaData(Util.camelToUpper(field.getName()),
                list.size() + 1, field.getType()));
      }
    }
    return list;
  }

  /** Creates the data dictionary, also called the information schema. It is a
   * schema called "metadata" that contains tables "TABLES", "COLUMNS" etc. */
  MapSchema createInformationSchema() {
    final MapSchema mapSchema =
        MapSchema.create(connection.getRootSchema(), "metadata");
    mapSchema.addTable(
        new TableInSchemaImpl(
            mapSchema,
            "TABLES",
            Schema.TableType.SYSTEM_TABLE,
            new MetadataTable<MetaTable>(
                connection,
                mapSchema,
                "TABLES",
                MetaTable.class) {
              public Enumerator<MetaTable> enumerator() {
                return schemas(connection.getCatalog())
                    .selectMany(
                        new Function1<
                            MetaSchema, Enumerable<MetaTable>>() {
                          public Enumerable<MetaTable> apply(
                              MetaSchema a0) {
                            return tablesAndTableFunctions(
                                a0, Functions.<String>truePredicate1());
                          }
                        })
                    .enumerator();
              }
            }));
    mapSchema.addTable(
        new TableInSchemaImpl(
            mapSchema,
            "COLUMNS",
            Schema.TableType.SYSTEM_TABLE,
            new MetadataTable<MetaColumn>(
                connection,
                mapSchema,
                "COLUMNS",
                MetaColumn.class) {
              public Enumerator<MetaColumn> enumerator() {
                return schemas(connection.getCatalog())
                    .selectMany(
                        new Function1<
                            MetaSchema, Enumerable<MetaTable>>() {
                          public Enumerable<MetaTable> apply(
                              MetaSchema a0) {
                            return tablesAndTableFunctions(
                                a0, Functions.<String>truePredicate1());
                          }
                        })
                    .selectMany(
                        new Function1<
                            MetaTable, Enumerable<MetaColumn>>() {
                          public Enumerable<MetaColumn> apply(
                              MetaTable a0) {
                            return columns(a0);
                          }
                        })
                    .enumerator();
              }
            }));
    return mapSchema;
  }

  /** Creates an empty result set. Useful for JDBC metadata methods that are
   * not implemented or which query entities that are not supported (e.g.
   * triggers in Lingual). */
  public static <E> ResultSet createEmptyResultSet(
      OptiqConnectionImpl connection,
      final Class<E> clazz) {
    return createResultSet(
        connection,
        fieldMetaData(clazz),
        new RecordEnumeratorCursor<E>(Linq4j.<E>emptyEnumerator(), clazz));
  }

  private static <E> ResultSet createResultSet(OptiqConnectionImpl connection,
      final List<ColumnMetaData> columnList,
      final Cursor cursor) {
    try {
      return connection.factory.newResultSet(
          connection.createStatement(),
          new OptiqPrepare.PrepareResult<E>("",
              ImmutableList.<AvaticaParameter>of(), null,
              columnList, -1, null, Object.class) {
            @Override
            public Cursor createCursor(DataContext dataContext) {
              return cursor;
            }
          },
          connection.getTimeZone()).execute();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static ResultSet createResultSet(
      OptiqConnectionImpl connection,
      final Enumerable<?> enumerable,
      final NamedFieldGetter columnGetter) {
    //noinspection unchecked
    return createResultSet(connection, columnGetter.columnNames,
        columnGetter.cursor(((Enumerable) enumerable).enumerator()));
  }

  public String getSqlKeywords() {
    return new SqlParser("").getParserImpl().getMetadata().getJdbcKeywords();
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

  public ResultSet getTables(
      String catalog,
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
    return createResultSet(
        connection,
        schemas(catalog)
            .where(schemaMatcher)
            .selectMany(
                new Function1<MetaSchema, Enumerable<MetaTable>>() {
                  public Enumerable<MetaTable> apply(MetaSchema a0) {
                    return tablesAndTableFunctions(
                        a0, matcher(tableNamePattern));
                  }
                })
            .where(typeFilter),
        new NamedFieldGetter(
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
            "REF_GENERATION"));
  }

  public ResultSet getColumns(
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern) {
    final Predicate1<String> tableNameMatcher = matcher(tableNamePattern);
    final Predicate1<MetaSchema> schemaMatcher = namedMatcher(schemaPattern);
    final Predicate1<MetaColumn> columnMatcher =
        namedMatcher(columnNamePattern);
    return createResultSet(connection,
        schemas(catalog)
            .where(schemaMatcher)
            .selectMany(
                new Function1<MetaSchema, Enumerable<MetaTable>>() {
                  public Enumerable<MetaTable> apply(MetaSchema a0) {
                    return tablesAndTableFunctions(
                        a0, tableNameMatcher);
                  }
                })
            .selectMany(
                new Function1<MetaTable, Enumerable<MetaColumn>>() {
                  public Enumerable<MetaColumn> apply(MetaTable a0) {
                    return columns(a0);
                  }
                })
            .where(columnMatcher),
        new NamedFieldGetter(
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
            "IS_GENERATEDCOLUMN"));
  }

  Enumerable<MetaCatalog> catalogs() {
    return Linq4j.asEnumerable(
        Arrays.asList(
            new MetaCatalog(connection.getCatalog())));
  }

  Enumerable<MetaTableType> tableTypes() {
    return Linq4j.asEnumerable(
        Arrays.asList(
            new MetaTableType("TABLE"),
            new MetaTableType("VIEW")));
  }

  Enumerable<MetaSchema> schemas(String catalog) {
    Collection<String> schemaNames =
        connection.rootSchema.getSubSchemaNames();
    return Linq4j.asEnumerable(schemaNames)
        .select(
            new Function1<String, MetaSchema>() {
              public MetaSchema apply(String name) {
                return new MetaSchema(
                    connection.rootSchema.getSubSchema(name),
                    connection.getCatalog(),
                    name);
              }
            })
        .orderBy(
            new Function1<MetaSchema, Comparable>() {
              public Comparable apply(MetaSchema metaSchema) {
                return (Comparable) FlatLists.of(
                    Util.first(metaSchema.tableCatalog, ""),
                    metaSchema.tableSchem);
              }
            }
        );
  }

  Enumerable<MetaTable> tables(final MetaSchema schema) {
    return Linq4j.asEnumerable(schema.optiqSchema.getTables().values())
        .select(
            new Function1<Schema.TableInSchema, MetaTable>() {
              public MetaTable apply(Schema.TableInSchema tableInSchema) {
                return new MetaTable(
                    tableInSchema.getTable(Object.class),
                    schema.tableCatalog, schema.tableSchem,
                    tableInSchema.name, tableInSchema.tableType.name());
              }
            });
  }

  Enumerable<MetaTable> tableFunctions(
      final MetaSchema schema,
      final Predicate1<String> matcher) {
    final List<Pair<String, TableFunction>> list =
        new ArrayList<Pair<String, TableFunction>>();
    for (Map.Entry<String, Schema.TableFunctionInSchema> entry
        : schema.optiqSchema.getTableFunctions().entries()) {
      if (matcher.apply(entry.getKey())) {
        final TableFunction tableFunction = entry.getValue().getTableFunction();
        if (tableFunction.getParameters().isEmpty()) {
          list.add(Pair.of(entry.getKey(), tableFunction));
        }
      }
    }
    return Linq4j
        .asEnumerable(list)
        .select(
            new Function1<Pair<String, TableFunction>, MetaTable>() {
              public MetaTable apply(Pair<String, TableFunction> a0) {
                final Table table =
                    a0.right.apply(Collections.emptyList());
                return new MetaTable(
                    table,
                    schema.tableCatalog,
                    schema.tableSchem,
                    a0.left,
                    Schema.TableType.VIEW.name());
              }
            });
  }

  Enumerable<MetaTable> tablesAndTableFunctions(
      final MetaSchema schema,
      final Predicate1<String> matcher) {
    return tables(schema)
        .where(
            new Predicate1<MetaTable>() {
              public boolean apply(MetaTable v1) {
                return matcher.apply(v1.getName());
              }
            })
        .concat(
            tableFunctions(schema, matcher));
  }

  public Enumerable<MetaColumn> columns(final MetaTable table) {
    return Linq4j.asEnumerable(table.optiqTable.getRowType().getFieldList())
        .select(
            new Function1<RelDataTypeField, MetaColumn>() {
              public MetaColumn apply(RelDataTypeField a0) {
                final int precision =
                    a0.getType().getSqlTypeName().allowsPrec()
                        && !(a0.getType()
                        instanceof RelDataTypeFactoryImpl.JavaType)
                        ? a0.getType().getPrecision()
                        : -1;
                return new MetaColumn(
                    table.tableCat,
                    table.tableSchem,
                    table.tableName,
                    a0.getName(),
                    a0.getType().getSqlTypeName().getJdbcOrdinal(),
                    a0.getType().getFullTypeString(),
                    precision,
                    a0.getType().getSqlTypeName().allowsScale()
                        ? a0.getType().getScale()
                        : null,
                    10,
                    a0.getType().isNullable()
                        ? DatabaseMetaData.columnNullable
                        : DatabaseMetaData.columnNoNulls,
                    precision,
                    a0.getIndex() + 1,
                    a0.getType().isNullable() ? "YES" : "NO");
              }
            }
        );
  }

  public ResultSet getSchemas(String catalog, Pat schemaPattern) {
    final Predicate1<MetaSchema> schemaMatcher = namedMatcher(schemaPattern);
    return createResultSet(
        connection,
        schemas(catalog)
            .where(schemaMatcher),
        new NamedFieldGetter(
            MetaSchema.class,
            "TABLE_SCHEM",
            "TABLE_CATALOG"));
  }

  public ResultSet getCatalogs() {
    return createResultSet(
        connection,
        catalogs(),
        new NamedFieldGetter(
            MetaCatalog.class,
            "TABLE_CATALOG"));
  }

  public ResultSet getTableTypes() {
    return createResultSet(
        connection,
        tableTypes(),
        new NamedFieldGetter(
            MetaTableType.class,
            "TABLE_TYPE"));
  }

  public ResultSet getProcedures(
      String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern) {
    return createEmptyResultSet(connection, MetaProcedure.class);
  }

  public ResultSet getProcedureColumns(
      String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern,
      Pat columnNamePattern) {
    return createEmptyResultSet(connection, MetaProcedureColumn.class);
  }

  public ResultSet getColumnPrivileges(
      String catalog,
      String schema,
      String table,
      Pat columnNamePattern) {
    return createEmptyResultSet(connection, MetaColumnPrivilege.class);
  }

  public ResultSet getTablePrivileges(
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern) {
    return createEmptyResultSet(connection, MetaTablePrivilege.class);
  }

  public ResultSet getBestRowIdentifier(
      String catalog,
      String schema,
      String table,
      int scope,
      boolean nullable) {
    return createEmptyResultSet(connection, MetaBestRowIdentifier.class);
  }

  public ResultSet getVersionColumns(
      String catalog, String schema, String table) {
    return createEmptyResultSet(connection, MetaVersionColumn.class);
  }

  public ResultSet getPrimaryKeys(
      String catalog, String schema, String table) {
    return createEmptyResultSet(connection, MetaPrimaryKey.class);
  }

  public ResultSet getImportedKeys(
      String catalog, String schema, String table) {
    return createEmptyResultSet(connection, MetaImportedKey.class);
  }

  public ResultSet getExportedKeys(
      String catalog, String schema, String table) {
    return createEmptyResultSet(connection, MetaExportedKey.class);
  }

  public ResultSet getCrossReference(
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable) {
    return createEmptyResultSet(connection, MetaCrossReference.class);
  }

  public ResultSet getTypeInfo() {
    return createEmptyResultSet(connection, MetaTypeInfo.class);
  }

  public ResultSet getIndexInfo(
      String catalog,
      String schema,
      String table,
      boolean unique,
      boolean approximate) {
    return createEmptyResultSet(connection, MetaIndexInfo.class);
  }

  public ResultSet getUDTs(
      String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      int[] types) {
    return createEmptyResultSet(connection, MetaUdt.class);
  }

  public ResultSet getSuperTypes(
      String catalog,
      Pat schemaPattern,
      Pat typeNamePattern) {
    return createEmptyResultSet(connection, MetaSuperType.class);
  }

  public ResultSet getSuperTables(
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern) {
    return createEmptyResultSet(connection, MetaSuperTable.class);
  }

  public ResultSet getAttributes(
      String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      Pat attributeNamePattern) {
    return createEmptyResultSet(connection, MetaAttribute.class);
  }

  public ResultSet getClientInfoProperties() {
    return createEmptyResultSet(connection, MetaClientInfoProperty.class);
  }

  public ResultSet getFunctions(
      String catalog,
      Pat schemaPattern,
      Pat functionNamePattern) {
    return createEmptyResultSet(connection, MetaFunction.class);
  }

  public ResultSet getFunctionColumns(
      String catalog,
      Pat schemaPattern,
      Pat functionNamePattern,
      Pat columnNamePattern) {
    return createEmptyResultSet(connection, MetaFunctionColumn.class);
  }

  public ResultSet getPseudoColumns(
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern) {
    return createEmptyResultSet(connection, MetaPseudoColumn.class);
  }

  interface Named {
    String getName();
  }

  public static class MetaColumn implements Named {
    public final String tableCat;
    public final String tableSchem;
    public final String tableName;
    public final String columnName;
    public final int dataType;
    public final String typeName;
    public final int columnSize;
    public final String bufferLength = null;
    public final Integer decimalDigits;
    public final int numPrecRadix;
    public final int nullable;
    public final String remarks = null;
    public final String columnDef = null;
    public final String sqlDataType = null;
    public final String sqlDatetimeSub = null;
    public final int charOctetLength;
    public final int ordinalPosition;
    public final String isNullable;
    public final String scopeCatalog = null;
    public final String scopeTable = null;
    public final String sourceDataType = null;
    public final String isAutoincrement = null;
    public final String isGeneratedcolumn = null;

    MetaColumn(
        String tableCat,
        String tableSchem,
        String tableName,
        String columnName,
        int dataType,
        String typeName,
        int columnSize,
        Integer decimalDigits,
        int numPrecRadix,
        int nullable,
        int charOctetLength,
        int ordinalPosition,
        String isNullable) {
      this.tableCat = tableCat;
      this.tableSchem = tableSchem;
      this.tableName = tableName;
      this.columnName = columnName;
      this.dataType = dataType;
      this.typeName = typeName;
      this.columnSize = columnSize;
      this.decimalDigits = decimalDigits;
      this.numPrecRadix = numPrecRadix;
      this.nullable = nullable;
      this.charOctetLength = charOctetLength;
      this.ordinalPosition = ordinalPosition;
      this.isNullable = isNullable;
    }

    public String getName() {
      return columnName;
    }
  }

  public static class MetaTable implements Named {
    private final Table optiqTable;
    public final String tableCat;
    public final String tableSchem;
    public final String tableName;
    public final String tableType;
    public final String remarks = null;
    public final String typeCat = null;
    public final String typeSchem = null;
    public final String typeName = null;
    public final String selfReferencingColName = null;
    public final String refGeneration = null;

    public MetaTable(
        Table optiqTable,
        String tableCat,
        String tableSchem,
        String tableName,
        String tableType) {
      this.optiqTable = optiqTable;
      assert optiqTable != null;
      this.tableCat = tableCat;
      this.tableSchem = tableSchem;
      this.tableName = tableName;
      this.tableType = tableType;
    }

    public String getName() {
      return tableName;
    }
  }

  public static class MetaSchema implements Named {
    private final Schema optiqSchema;
    public final String tableCatalog;
    public final String tableSchem;

    public MetaSchema(
        Schema optiqSchema,
        String tableCatalog,
        String tableSchem) {
      this.optiqSchema = optiqSchema;
      this.tableCatalog = tableCatalog;
      this.tableSchem = tableSchem;
    }

    public String getName() {
      return tableSchem;
    }
  }

  public static class MetaCatalog implements Named {
    public final String tableCatalog;

    public MetaCatalog(
        String tableCatalog) {
      this.tableCatalog = tableCatalog;
    }

    public String getName() {
      return tableCatalog;
    }
  }

  public static class MetaTableType {
    public final String tableType;

    public MetaTableType(String tableType) {
      this.tableType = tableType;
    }
  }

  public static class MetaProcedure {
  }

  public static class MetaProcedureColumn {
  }

  public static class MetaColumnPrivilege {
  }

  public static class MetaTablePrivilege {
  }

  public static class MetaBestRowIdentifier {
  }

  public static class MetaVersionColumn {
    public final short scope;
    public final String columnName;
    public final int dataType;
    public final String typeName;
    public final int columnSize;
    public final int bufferLength;
    public final short decimalDigits;
    public final short pseudoColumn;

    MetaVersionColumn(short scope, String columnName, int dataType,
        String typeName, int columnSize, int bufferLength, short decimalDigits,
        short pseudoColumn) {
      this.scope = scope;
      this.columnName = columnName;
      this.dataType = dataType;
      this.typeName = typeName;
      this.columnSize = columnSize;
      this.bufferLength = bufferLength;
      this.decimalDigits = decimalDigits;
      this.pseudoColumn = pseudoColumn;
    }
  }

  public static class MetaPrimaryKey {
    public final String tableCat;
    public final String tableSchem;
    public final String tableName;
    public final String columnName;
    public final short keySeq;
    public final String pkName;

    MetaPrimaryKey(String tableCat, String tableSchem, String tableName,
        String columnName, short keySeq, String pkName) {
      this.tableCat = tableCat;
      this.tableSchem = tableSchem;
      this.tableName = tableName;
      this.columnName = columnName;
      this.keySeq = keySeq;
      this.pkName = pkName;
    }
  }

  public static class MetaImportedKey {
  }

  public static class MetaExportedKey {
  }

  public static class MetaCrossReference {
  }

  public static class MetaTypeInfo {
  }

  public static class MetaIndexInfo {
  }

  public static class MetaUdt {
  }

  public static class MetaSuperType {
  }

  public static class MetaAttribute {
  }

  public static class MetaClientInfoProperty {
  }

  public static class MetaFunction {
  }

  public static class MetaFunctionColumn {
  }

  public static class MetaPseudoColumn {
  }

  public static class MetaSuperTable {
  }

  private static class NamedFieldGetter {
    private final List<Field> fields = new ArrayList<Field>();
    private final List<ColumnMetaData> columnNames =
        new ArrayList<ColumnMetaData>();

    public NamedFieldGetter(Class clazz, String... names) {
      for (String name : names) {
        final int index = fields.size();
        final String fieldName = Util.toCamelCase(name);
        final Field field;
        try {
          field = clazz.getField(fieldName);
        } catch (NoSuchFieldException e) {
          throw new RuntimeException(e);
        }
        columnNames.add(columnMetaData(name, index, field.getType()));
        fields.add(field);
      }
    }

    Object get(Object o, int columnIndex) {
      try {
        return fields.get(columnIndex).get(o);
      } catch (IllegalArgumentException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    public Cursor cursor(final Enumerator<Object> enumerator) {
      return new AbstractCursor() {
        protected Getter createGetter(final int ordinal) {
          return new Getter() {
            public Object getObject() {
              return get(enumerator.current(), ordinal);
            }

            public boolean wasNull() {
              return getObject() == null;
            }
          };
        }

        public boolean next() {
          return enumerator.moveNext();
        }

        public void close() {
          enumerator.close();
        }
      };
    }
  }

  private static abstract class MetadataTable<E>
      extends AbstractQueryable<E>
      implements Table<E> {
    private final MapSchema schema;
    private final String tableName;
    private final Class<E> clazz;
    private final OptiqConnectionImpl connection;

    public MetadataTable(
        OptiqConnectionImpl connection, MapSchema schema, String tableName,
        Class<E> clazz) {
      super();
      this.schema = schema;
      this.tableName = tableName;
      this.clazz = clazz;
      this.connection = connection;
    }

    public RelDataType getRowType() {
      return connection.typeFactory.createType(getElementType());
    }

    public Class<E> getElementType() {
      return clazz;
    }

    public Expression getExpression() {
      return Expressions.call(
          schema.getExpression(),
          "getTable",
          Expressions.constant(tableName),
          Expressions.constant(getElementType()));
    }

    public QueryProvider getProvider() {
      return connection;
    }

    public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }

    public Iterator<E> iterator() {
      return Linq4j.enumeratorIterator(enumerator());
    }
  }
}

// End MetaImpl.java
