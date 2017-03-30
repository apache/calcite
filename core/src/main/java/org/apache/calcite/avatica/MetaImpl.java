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
package org.apache.calcite.avatica;

import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.util.ArrayIteratorCursor;
import org.apache.calcite.avatica.util.Cursor;
import org.apache.calcite.avatica.util.IteratorCursor;
import org.apache.calcite.avatica.util.ListIteratorCursor;
import org.apache.calcite.avatica.util.MapIteratorCursor;
import org.apache.calcite.avatica.util.RecordIteratorCursor;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Basic implementation of {@link Meta}.
 *
 * <p>Each sub-class must implement the two remaining abstract methods,
 * {@link #prepare} and
 * {@link #prepareAndExecute}.
 * It should also override metadata methods such as {@link #getCatalogs(Meta.ConnectionHandle)} and
 * {@link #getTables} for the element types for which it has instances; the
 * default metadata methods return empty collections.
 */
public abstract class MetaImpl implements Meta {
  /** The {@link AvaticaConnection} backing {@code this}. */
  protected final AvaticaConnection connection;
  /** Represents the various states specific to {@link #connection}.
   *
   * <p>Note: this instance is used recursively with {@link #connection}'s getter and setter
   * methods.</p>
   */
  protected final ConnectionPropertiesImpl connProps;

  public MetaImpl(AvaticaConnection connection) {
    this.connection = connection;
    this.connProps = new ConnectionPropertiesImpl();
  }

  /** Uses a {@link org.apache.calcite.avatica.Meta.CursorFactory} to convert
   * an {@link Iterable} into a
   * {@link org.apache.calcite.avatica.util.Cursor}. */
  public static Cursor createCursor(CursorFactory cursorFactory,
      Iterable<Object> iterable) {
    switch (cursorFactory.style) {
    case OBJECT:
      return new IteratorCursor<Object>(iterable.iterator()) {
        protected Getter createGetter(int ordinal) {
          return new ObjectGetter(ordinal);
        }
      };
    case ARRAY:
      @SuppressWarnings("unchecked") final Iterable<Object[]> iterable1 =
          (Iterable<Object[]>) (Iterable) iterable;
      return new ArrayIteratorCursor(iterable1.iterator());
    case RECORD:
      @SuppressWarnings("unchecked") final Class<Object> clazz =
          cursorFactory.clazz;
      return new RecordIteratorCursor<Object>(iterable.iterator(), clazz);
    case RECORD_PROJECTION:
      @SuppressWarnings("unchecked") final Class<Object> clazz2 =
          cursorFactory.clazz;
      return new RecordIteratorCursor<Object>(iterable.iterator(), clazz2,
          cursorFactory.fields);
    case LIST:
      @SuppressWarnings("unchecked") final Iterable<List<Object>> iterable2 =
          (Iterable<List<Object>>) (Iterable) iterable;
      return new ListIteratorCursor(iterable2.iterator());
    case MAP:
      @SuppressWarnings("unchecked") final Iterable<Map<String, Object>>
          iterable3 =
          (Iterable<Map<String, Object>>) (Iterable) iterable;
      return new MapIteratorCursor(iterable3.iterator(),
          cursorFactory.fieldNames);
    default:
      throw new AssertionError("unknown style: " + cursorFactory.style);
    }
  }

  public static List<List<Object>> collect(CursorFactory cursorFactory,
      final Iterator<Object> iterator, List<List<Object>> list) {
    final Iterable<Object> iterable = new Iterable<Object>() {
      public Iterator<Object> iterator() {
        return iterator;
      }
    };
    return collect(cursorFactory, iterable, list);
  }

  public static List<List<Object>> collect(CursorFactory cursorFactory,
      Iterable<Object> iterable, final List<List<Object>> list) {
    switch (cursorFactory.style) {
    case OBJECT:
      for (Object o : iterable) {
        list.add(Collections.singletonList(o));
      }
      return list;
    case ARRAY:
      @SuppressWarnings("unchecked") final Iterable<Object[]> iterable1 =
          (Iterable<Object[]>) (Iterable) iterable;
      for (Object[] objects : iterable1) {
        list.add(Arrays.asList(objects));
      }
      return list;
    case RECORD:
    case RECORD_PROJECTION:
      final Field[] fields;
      switch (cursorFactory.style) {
      case RECORD:
        fields = cursorFactory.clazz.getFields();
        break;
      default:
        fields = cursorFactory.fields.toArray(
            new Field[cursorFactory.fields.size()]);
      }
      for (Object o : iterable) {
        final Object[] objects = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
          Field field = fields[i];
          try {
            objects[i] = field.get(o);
          } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }
        list.add(Arrays.asList(objects));
      }
      return list;
    case LIST:
      @SuppressWarnings("unchecked") final Iterable<List<Object>> iterable2 =
          (Iterable<List<Object>>) (Iterable) iterable;
      for (List<Object> objects : iterable2) {
        list.add(objects);
      }
      return list;
    case MAP:
      @SuppressWarnings("unchecked") final Iterable<Map<String, Object>>
          iterable3 =
          (Iterable<Map<String, Object>>) (Iterable) iterable;
      for (Map<String, Object> map : iterable3) {
        final List<Object> objects = new ArrayList<Object>();
        for (String fieldName : cursorFactory.fieldNames) {
          objects.add(map.get(fieldName));
        }
        list.add(objects);
      }
      return list;
    default:
      throw new AssertionError("unknown style: " + cursorFactory.style);
    }
  }

  @Override public void openConnection(ConnectionHandle ch, Map<String, String> info) {
    // dummy implementation, connection is already created at this point
  }

  @Override public void closeConnection(ConnectionHandle ch) {
    // TODO: implement
    //
    // lots of Calcite tests break with this simple implementation,
    // requires investigation

//    try {
//      connection.close();
//    } catch (SQLException e) {
//      throw new RuntimeException(e);
//    }
  }

  @Override public ConnectionProperties connectionSync(ConnectionHandle ch,
      ConnectionProperties connProps) {
    this.connProps.merge(connProps);
    this.connProps.setDirty(false);
    return this.connProps;
  }

  public StatementHandle createStatement(ConnectionHandle ch) {
    return new StatementHandle(ch.id, connection.statementCount++, null);
  }

  /** Creates an empty result set. Useful for JDBC metadata methods that are
   * not implemented or which query entities that are not supported (e.g.
   * triggers in Lingual). */
  protected <E> MetaResultSet createEmptyResultSet(final Class<E> clazz) {
    return createResultSet(Collections.<String, Object>emptyMap(),
        fieldMetaData(clazz).columns,
        CursorFactory.deduce(fieldMetaData(clazz).columns, null),
        Frame.EMPTY);
  }

  public static ColumnMetaData columnMetaData(String name, int index,
      Class<?> type, boolean columnNullable) {
    return columnMetaData(name, index, type, columnNullable
        ? DatabaseMetaData.columnNullable
        : DatabaseMetaData.columnNoNulls);
  }

  public static ColumnMetaData columnMetaData(String name, int index,
      Class<?> type, int columnNullable) {
    TypeInfo pair = TypeInfo.m.get(type);
    ColumnMetaData.Rep rep =
        ColumnMetaData.Rep.VALUE_MAP.get(type);
    ColumnMetaData.AvaticaType scalarType =
        ColumnMetaData.scalar(pair.sqlType, pair.sqlTypeName, rep);
    return new ColumnMetaData(
        index, false, true, false, false,
        columnNullable,
        true, -1, name, name, null,
        0, 0, null, null, scalarType, true, false, false,
        scalarType.columnClassName());
  }

  protected static ColumnMetaData.StructType fieldMetaData(Class<?> clazz) {
    final List<ColumnMetaData> list = new ArrayList<ColumnMetaData>();
    for (Field field : clazz.getFields()) {
      if (Modifier.isPublic(field.getModifiers())
          && !Modifier.isStatic(field.getModifiers())) {
        int columnNullable = getColumnNullability(field);
        list.add(
            columnMetaData(
                AvaticaUtils.camelToUpper(field.getName()),
                list.size(), field.getType(), columnNullable));
      }
    }
    return ColumnMetaData.struct(list);
  }

  protected static int getColumnNullability(Field field) {
    // Check annotations first
    if (field.isAnnotationPresent(ColumnNoNulls.class)) {
      return DatabaseMetaData.columnNoNulls;
    }

    if (field.isAnnotationPresent(ColumnNullable.class)) {
      return DatabaseMetaData.columnNullable;
    }

    if (field.isAnnotationPresent(ColumnNullableUnknown.class)) {
      return DatabaseMetaData.columnNullableUnknown;
    }

    // check the field type to decide if annotated, as a fallback
    if (field.getType().isPrimitive()) {
      return DatabaseMetaData.columnNoNulls;
    }

    return DatabaseMetaData.columnNullable;
  }

  protected MetaResultSet createResultSet(
      Map<String, Object> internalParameters, List<ColumnMetaData> columns,
      CursorFactory cursorFactory, Frame firstFrame) {
    try {
      final AvaticaStatement statement = connection.createStatement();
      final Signature signature =
          new Signature(columns, "", Collections.<AvaticaParameter>emptyList(),
              internalParameters, cursorFactory, Meta.StatementType.SELECT);
      return MetaResultSet.create(connection.id, statement.getId(), true,
          signature, firstFrame);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /** An object that has a name. */
  public interface Named {
    @JsonIgnore String getName();
  }

  /** Annotation that indicates that a meta field may contain null values. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.FIELD)
  public @interface ColumnNullable {
  }

  /** Annotation that indicates that it is unknown whether a meta field may contain
   * null values. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.FIELD)
  public @interface ColumnNullableUnknown {
  }

  /** Annotation that indicates that a meta field may not contain null
   * values. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.FIELD)
  public @interface ColumnNoNulls {
  }

  /** Metadata describing a column. */
  public static class MetaColumn implements Named {
    public final String tableCat;
    public final String tableSchem;
    @ColumnNoNulls
    public final String tableName;
    @ColumnNoNulls
    public final String columnName;
    public final int dataType;
    @ColumnNoNulls
    public final String typeName;
    public final Integer columnSize;
    @ColumnNullableUnknown
    public final Integer bufferLength = null;
    public final Integer decimalDigits;
    public final Integer numPrecRadix;
    public final int nullable;
    public final String remarks = null;
    public final String columnDef = null;
    @ColumnNullableUnknown
    public final Integer sqlDataType = null;
    @ColumnNullableUnknown
    public final Integer sqlDatetimeSub = null;
    public final Integer charOctetLength;
    public final int ordinalPosition;
    @ColumnNoNulls
    public final String isNullable;
    public final String scopeCatalog = null;
    public final String scopeSchema = null;
    public final String scopeTable = null;
    public final Short sourceDataType = null;
    @ColumnNoNulls
    public final String isAutoincrement = "";
    @ColumnNoNulls
    public final String isGeneratedcolumn = "";

    public MetaColumn(
        String tableCat,
        String tableSchem,
        String tableName,
        String columnName,
        int dataType,
        String typeName,
        Integer columnSize,
        Integer decimalDigits,
        Integer numPrecRadix,
        int nullable,
        Integer charOctetLength,
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

  /** Metadata describing a table. */
  public static class MetaTable implements Named {
    public final String tableCat;
    public final String tableSchem;
    @ColumnNoNulls
    public final String tableName;
    @ColumnNoNulls
    public final String tableType;
    public final String remarks = null;
    public final String typeCat = null;
    public final String typeSchem = null;
    public final String typeName = null;
    public final String selfReferencingColName = null;
    public final String refGeneration = null;

    public MetaTable(
        String tableCat,
        String tableSchem,
        String tableName,
        String tableType) {
      this.tableCat = tableCat;
      this.tableSchem = tableSchem;
      this.tableName = tableName;
      this.tableType = tableType;
    }

    public String getName() {
      return tableName;
    }
  }

  /** Metadata describing a schema. */
  public static class MetaSchema implements Named {
    @ColumnNoNulls
    public final String tableSchem;
    public final String tableCatalog;

    public MetaSchema(
        String tableCatalog,
        String tableSchem) {
      this.tableCatalog = tableCatalog;
      this.tableSchem = tableSchem;
    }

    public String getName() {
      return tableSchem;
    }
  }

  /** Metadata describing a catalog. */
  public static class MetaCatalog implements Named {
    @ColumnNoNulls
    public final String tableCat;

    public MetaCatalog(
        String tableCatalog) {
      this.tableCat = tableCatalog;
    }

    public String getName() {
      return tableCat;
    }
  }

  /** Metadata describing a table type. */
  public static class MetaTableType {
    @ColumnNoNulls
    public final String tableType;

    public MetaTableType(String tableType) {
      this.tableType = tableType;
    }
  }

  /** Metadata describing a procedure. */
  public static class MetaProcedure {
    public final String procedureCat;
    public final String procedureSchem;
    @ColumnNoNulls
    public final String procedureName;
    public final String futureUse1 = null;
    public final String futureUse2 = null;
    public final String futureUse3 = null;
    public final String remarks = null;
    public final short procedureType;
    public final String specificName;

    public MetaProcedure(String procedureCat, String procedureSchem, String procedureName,
        short procedureType, String specificName) {
      this.procedureCat = procedureCat;
      this.procedureSchem = procedureSchem;
      this.procedureName = procedureName;
      this.procedureType = procedureType;
      this.specificName = specificName;
    }
  }

  /** Metadata describing a procedure column. */
  public static class MetaProcedureColumn {
    public final String procedureCat;
    public final String procedureSchem;
    @ColumnNoNulls
    public final String procedureName;
    @ColumnNoNulls
    public final String columnName;
    public final short columnType;
    public final int dataType;
    @ColumnNoNulls
    public final String typeName;
    public final Integer precision;
    public final Integer length;
    public final Short scale;
    public final Short radix;
    public final short nullable;
    public final String remarks = null;
    public final String columnDef;
    @ColumnNullableUnknown
    public final Integer sqlDataType = null;
    @ColumnNullableUnknown
    public final Integer sqlDatetimeSub = null;
    public final Integer charOctetLength;
    public final int ordinalPosition;
    @ColumnNoNulls
    public final String isNullable;
    public final String specificName;

    public MetaProcedureColumn(
        String procedureCat,
        String procedureSchem,
        String procedureName,
        String columnName,
        short columnType,
        int dataType,
        String typeName,
        Integer precision,
        Integer length,
        Short scale,
        Short radix,
        short nullable,
        String columnDef,
        Integer charOctetLength,
        int ordinalPosition,
        String isNullable,
        String specificName) {
      this.procedureCat = procedureCat;
      this.procedureSchem = procedureSchem;
      this.procedureName = procedureName;
      this.columnName = columnName;
      this.columnType = columnType;
      this.dataType = dataType;
      this.typeName = typeName;
      this.precision = precision;
      this.length = length;
      this.scale = scale;
      this.radix = radix;
      this.nullable = nullable;
      this.columnDef = columnDef;
      this.charOctetLength = charOctetLength;
      this.ordinalPosition = ordinalPosition;
      this.isNullable = isNullable;
      this.specificName = specificName;
    }
  }

  /** Metadata describing a column privilege. */
  public static class MetaColumnPrivilege {
    public final String tableCat;
    public final String tableSchem;
    @ColumnNoNulls
    public final String tableName;
    @ColumnNoNulls
    public final String columnName;
    public final String grantor;
    @ColumnNoNulls
    public final String grantee;
    @ColumnNoNulls
    public final String privilege;
    public final String isGrantable;

    public MetaColumnPrivilege(
        String tableCat,
        String tableSchem,
        String tableName,
        String columnName,
        String grantor,
        String grantee,
        String privilege,
        String isGrantable) {
      this.tableCat = tableCat;
      this.tableSchem = tableSchem;
      this.tableName = tableName;
      this.columnName = columnName;
      this.grantor = grantor;
      this.grantee = grantee;
      this.privilege = privilege;
      this.isGrantable = isGrantable;
    }
  }

  /** Metadata describing a table privilege. */
  public static class MetaTablePrivilege {
    public final String tableCat;
    public final String tableSchem;
    @ColumnNoNulls
    public final String tableName;

    public final String grantor;
    @ColumnNoNulls
    public final String grantee;
    @ColumnNoNulls
    public final String privilege;
    public final String isGrantable;

    public MetaTablePrivilege(
        String tableCat,
        String tableSchem,
        String tableName,
        String grantor,
        String grantee,
        String privilege,
        String isGrantable) {
      this.tableCat = tableCat;
      this.tableSchem = tableSchem;
      this.tableName = tableName;
      this.grantor = grantor;
      this.grantee = grantee;
      this.privilege = privilege;
      this.isGrantable = isGrantable;
    }
  }

  /** Metadata describing the best identifier for a row. */
  public static class MetaBestRowIdentifier {
    public final short scope;
    @ColumnNoNulls
    public final String columnName;
    public final int dataType;
    @ColumnNoNulls
    public final String typeName;
    public final Integer columnSize;
    @ColumnNullableUnknown
    public final Integer bufferLength = null;
    public final Short decimalDigits;
    public short pseudoColumn;

    public MetaBestRowIdentifier(
        short scope,
        String columnName,
        int dataType,
        String typeName,
        Integer columnSize,
        Short decimalDigits,
        short pseudoColumn) {
      this.scope = scope;
      this.columnName = columnName;
      this.dataType = dataType;
      this.typeName = typeName;
      this.columnSize = columnSize;
      this.decimalDigits = decimalDigits;
      this.pseudoColumn = pseudoColumn;
    }
  }

  /** Metadata describing a version column. */
  public static class MetaVersionColumn {
    @ColumnNullableUnknown
    public final Short scope;
    @ColumnNoNulls
    public final String columnName;
    public final int dataType;
    @ColumnNoNulls
    public final String typeName;
    public final Integer columnSize;
    public final Integer bufferLength;
    public final Short decimalDigits;
    public final short pseudoColumn;

    MetaVersionColumn(
        Short scope,
        String columnName,
        int dataType,
        String typeName,
        Integer columnSize,
        Integer bufferLength,
        Short decimalDigits,
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

  /** Metadata describing a primary key. */
  public static class MetaPrimaryKey {
    public final String tableCat;
    public final String tableSchem;
    @ColumnNoNulls
    public final String tableName;
    @ColumnNoNulls
    public final String columnName;
    public final short keySeq;
    public final String pkName;

    MetaPrimaryKey(
        String tableCat,
        String tableSchem,
        String tableName,
        String columnName,
        short keySeq,
        String pkName) {
      this.tableCat = tableCat;
      this.tableSchem = tableSchem;
      this.tableName = tableName;
      this.columnName = columnName;
      this.keySeq = keySeq;
      this.pkName = pkName;
    }
  }

  /** Metadata describing an imported key. */
  public static class MetaImportedKey {
    public final String pktableCat;
    public final String pktableSchem;
    @ColumnNoNulls
    public final String pktableName;
    @ColumnNoNulls
    public final String pkcolumnName;
    public final String fktableCat;
    public final String fktableSchem;
    @ColumnNoNulls
    public final String fktableName;
    @ColumnNoNulls
    public final String fkcolumnName;
    public final short keySeq;
    public final short updateRule;
    public final short deleteRule;
    public final String fkName;
    public final String pkName;
    public final short deferability;

    public MetaImportedKey(
        String pktableCat,
        String pktableSchem,
        String pktableName,
        String pkcolumnName,
        String fktableCat,
        String fktableSchem,
        String fktableName,
        String fkcolumnName,
        short keySeq,
        short updateRule,
        short deleteRule,
        String fkName,
        String pkName,
        short deferability) {
      this.pktableCat = pktableCat;
      this.pktableSchem = pktableSchem;
      this.pktableName = pktableName;
      this.pkcolumnName = pkcolumnName;
      this.fktableCat = fktableCat;
      this.fktableSchem = fktableSchem;
      this.fktableName = fktableName;
      this.fkcolumnName = fkcolumnName;
      this.keySeq = keySeq;
      this.updateRule = updateRule;
      this.deleteRule = deleteRule;
      this.fkName = fkName;
      this.pkName = pkName;
      this.deferability = deferability;
    }
  }

  /** Metadata describing an exported key. */
  public static class MetaExportedKey {
    public final String pktableCat;
    public final String pktableSchem;
    @ColumnNoNulls
    public final String pktableName;
    @ColumnNoNulls
    public final String pkcolumnName;
    public final String fktableCat;
    public final String fktableSchem;
    @ColumnNoNulls
    public final String fktableName;
    @ColumnNoNulls
    public final String fkcolumnName;
    public final short keySeq;
    public final short updateRule;
    public final short deleteRule;
    public final String fkName;
    public final String pkName;
    public final short deferability;

    public MetaExportedKey(
        String pktableCat,
        String pktableSchem,
        String pktableName,
        String pkcolumnName,
        String fktableCat,
        String fktableSchem,
        String fktableName,
        String fkcolumnName,
        short keySeq,
        short updateRule,
        short deleteRule,
        String fkName,
        String pkName,
        short deferability) {
      this.pktableCat = pktableCat;
      this.pktableSchem = pktableSchem;
      this.pktableName = pktableName;
      this.pkcolumnName = pkcolumnName;
      this.fktableCat = fktableCat;
      this.fktableSchem = fktableSchem;
      this.fktableName = fktableName;
      this.fkcolumnName = fkcolumnName;
      this.keySeq = keySeq;
      this.updateRule = updateRule;
      this.deleteRule = deleteRule;
      this.fkName = fkName;
      this.pkName = pkName;
      this.deferability = deferability;
    }
  }

  /** Metadata describing a cross reference. */
  public static class MetaCrossReference {
    public final String pktableCat;
    public final String pktableSchem;
    @ColumnNoNulls
    public final String pktableName;
    @ColumnNoNulls
    public final String pkcolumnName;
    public final String fktableCat;
    public final String fktableSchem;
    @ColumnNoNulls
    public final String fktableName;
    @ColumnNoNulls
    public final String fkcolumnName;
    public final short keySeq;
    public final short updateRule;
    public final short deleteRule;
    public final String fkName;
    public final String pkName;
    public final short deferability;

    public MetaCrossReference(
        String pktableCat,
        String pktableSchem,
        String pktableName,
        String pkcolumnName,
        String fktableCat,
        String fktableSchem,
        String fktableName,
        String fkcolumnName,
        short keySeq,
        short updateRule,
        short deleteRule,
        String fkName,
        String pkName,
        short deferability) {
      this.pktableCat = pktableCat;
      this.pktableSchem = pktableSchem;
      this.pktableName = pktableName;
      this.pkcolumnName = pkcolumnName;
      this.fktableCat = fktableCat;
      this.fktableSchem = fktableSchem;
      this.fktableName = fktableName;
      this.fkcolumnName = fkcolumnName;
      this.keySeq = keySeq;
      this.updateRule = updateRule;
      this.deleteRule = deleteRule;
      this.fkName = fkName;
      this.pkName = pkName;
      this.deferability = deferability;
    }
  }

  /** Metadata describing type info. */
  public static class MetaTypeInfo implements Named {
    @ColumnNoNulls
    public final String typeName;
    public final int dataType;
    public final Integer precision;
    public final String literalPrefix;
    public final String literalSuffix;
    //TODO: Add create parameter for type on DDL
    public final String createParams = null;
    public final short nullable;
    public final boolean caseSensitive;
    public final short searchable;
    public final boolean unsignedAttribute;
    public final boolean fixedPrecScale;
    public final boolean autoIncrement;
    public final String localTypeName;
    public final Short minimumScale;
    public final Short maximumScale;
    @ColumnNullableUnknown
    public final Integer sqlDataType = null;
    @ColumnNullableUnknown
    public final Integer sqlDatetimeSub = null;
    public final Integer numPrecRadix; //nullable int

    public MetaTypeInfo(
        String typeName,
        int dataType,
        Integer precision,
        String literalPrefix,
        String literalSuffix,
        short nullable,
        boolean caseSensitive,
        short searchable,
        boolean unsignedAttribute,
        boolean fixedPrecScale,
        boolean autoIncrement,
        Short minimumScale,
        Short maximumScale,
        Integer numPrecRadix) {
      this.typeName = typeName;
      this.dataType = dataType;
      this.precision = precision;
      this.literalPrefix = literalPrefix;
      this.literalSuffix = literalSuffix;
      this.nullable = nullable;
      this.caseSensitive = caseSensitive;
      this.searchable = searchable;
      this.unsignedAttribute = unsignedAttribute;
      this.fixedPrecScale = fixedPrecScale;
      this.autoIncrement = autoIncrement;
      this.localTypeName = typeName;
      this.minimumScale = minimumScale;
      this.maximumScale = maximumScale;
      this.numPrecRadix = numPrecRadix == 0 ? null : numPrecRadix;
    }

    public String getName() {
      return typeName;
    }
  }

  /** Metadata describing index info. */
  public static class MetaIndexInfo {
    public final String tableCat;
    public final String tableSchem;
    @ColumnNoNulls
    public final String tableName;
    public final boolean nonUnique;
    public final String indexQualifier;
    public final String indexName;
    public final short type;
    public final short ordinalPosition;
    public final String columnName;
    public final String ascOrDesc;
    public final long cardinality;
    public final long pages;
    public final String filterCondition;

    public MetaIndexInfo(
        String tableCat,
        String tableSchem,
        String tableName,
        boolean nonUnique,
        String indexQualifier,
        String indexName,
        short type,
        short ordinalPosition,
        String columnName,
        String ascOrDesc,
        long cardinality,
        long pages,
        String filterCondition) {
      this.tableCat = tableCat;
      this.tableSchem = tableSchem;
      this.tableName = tableName;
      this.nonUnique = nonUnique;
      this.indexQualifier = indexQualifier;
      this.indexName = indexName;
      this.type = type;
      this.ordinalPosition = ordinalPosition;
      this.columnName = columnName;
      this.ascOrDesc = ascOrDesc;
      this.cardinality = cardinality;
      this.pages = pages;
      this.filterCondition = filterCondition;
    }
  }

  /** Metadata describing a user-defined type. */
  public static class MetaUdt {
    public final String typeCat;
    public final String typeSchem;
    @ColumnNoNulls
    public final String typeName;
    @ColumnNoNulls
    public final String className;
    public final int dataType;
    public final String remarks = null;
    public final Short baseType;

    public MetaUdt(
        String typeCat,
        String typeSchem,
        String typeName,
        String className,
        int dataType,
        Short baseType) {
      this.typeCat = typeCat;
      this.typeSchem = typeSchem;
      this.typeName = typeName;
      this.className = className;
      this.dataType = dataType;
      this.baseType = baseType;
    }
  }

  /** Metadata describing a super-type. */
  public static class MetaSuperType {
    public final String typeCat;
    public final String typeSchem;
    @ColumnNoNulls
    public final String typeName;
    public final String supertypeCat;
    public final String supertypeSchem;
    @ColumnNoNulls
    public final String supertypeName;

    public MetaSuperType(
        String typeCat,
        String typeSchem,
        String typeName,
        String supertypeCat,
        String supertypeSchem,
        String supertypeName) {
      this.typeCat = typeCat;
      this.typeSchem = typeSchem;
      this.typeName = typeName;
      this.supertypeCat = supertypeCat;
      this.supertypeSchem = supertypeSchem;
      this.supertypeName = supertypeName;
    }
  }

  /** Metadata describing an attribute. */
  public static class MetaAttribute {
    public final String typeCat;
    public final String typeSchem;
    @ColumnNoNulls
    public final String typeName;
    @ColumnNoNulls
    public final String attrName;
    public final int dataType;
    @ColumnNoNulls
    public String attrTypeName;
    public final Integer attrSize;
    public final Integer decimalDigits;
    public final Integer numPrecRadix;
    public final int nullable;
    public final String remarks = null;
    public final String attrDef = null;
    @ColumnNullableUnknown
    public final Integer sqlDataType = null;
    @ColumnNullableUnknown
    public final Integer sqlDatetimeSub = null;
    public final Integer charOctetLength;
    public final int ordinalPosition;
    @ColumnNoNulls
    public final String isNullable;
    public final String scopeCatalog = null;
    public final String scopeSchema = null;
    public final String scopeTable = null;
    public final Short sourceDataType = null;

    public MetaAttribute(
        String typeCat,
        String typeSchem,
        String typeName,
        String attrName,
        int dataType,
        String attrTypeName,
        Integer attrSize,
        Integer decimalDigits,
        Integer numPrecRadix,
        int nullable,
        Integer charOctetLength,
        int ordinalPosition,
        String isNullable) {
      this.typeCat = typeCat;
      this.typeSchem = typeSchem;
      this.typeName = typeName;
      this.attrName = attrName;
      this.dataType = dataType;
      this.attrTypeName = attrTypeName;
      this.attrSize = attrSize;
      this.decimalDigits = decimalDigits;
      this.numPrecRadix = numPrecRadix;
      this.nullable = nullable;
      this.charOctetLength = charOctetLength;
      this.ordinalPosition = ordinalPosition;
      this.isNullable = isNullable;
    }
  }

  /** Metadata describing a client info property. */
  public static class MetaClientInfoProperty {
    @ColumnNoNulls
    public final String name;
    public final int maxLen;
    public final String defaultValue;
    public final String description;

    public MetaClientInfoProperty(
        String name,
        int maxLen,
        String defaultValue,
        String description) {
      this.name = name;
      this.maxLen = maxLen;
      this.defaultValue = defaultValue;
      this.description = description;
    }
  }

  /** Metadata describing a function. */
  public static class MetaFunction {
    public final String functionCat;
    public final String functionSchem;
    @ColumnNoNulls
    public final String functionName;
    public final String remarks = null;
    public final short functionType;
    public final String specificName;

    public MetaFunction(
        String functionCat,
        String functionSchem,
        String functionName,
        short functionType,
        String specificName) {
      this.functionCat = functionCat;
      this.functionSchem = functionSchem;
      this.functionName = functionName;
      this.functionType = functionType;
      this.specificName = specificName;
    }
  }

  /** Metadata describing a function column. */
  public static class MetaFunctionColumn {
    public final String functionCat;
    public final String functionSchem;
    @ColumnNoNulls
    public final String functionName;
    @ColumnNoNulls
    public final String columnName;
    public final short columnType;
    public final int dataType;
    @ColumnNoNulls
    public final String typeName;
    public final Integer precision;
    public final Integer length;
    public final Short scale;
    public final Short radix;
    public final short nullable;
    public final String remarks = null;
    public final Integer charOctetLength;
    public final int ordinalPosition;
    @ColumnNoNulls
    public final String isNullable;
    public final String specificName;

    public MetaFunctionColumn(
        String functionCat,
        String functionSchem,
        String functionName,
        String columnName,
        short columnType,
        int dataType,
        String typeName,
        Integer precision,
        Integer length,
        Short scale,
        Short radix,
        short nullable,
        Integer charOctetLength,
        int ordinalPosition,
        String isNullable,
        String specificName) {
      this.functionCat = functionCat;
      this.functionSchem = functionSchem;
      this.functionName = functionName;
      this.columnName = columnName;
      this.columnType = columnType;
      this.dataType = dataType;
      this.typeName = typeName;
      this.precision = precision;
      this.length = length;
      this.scale = scale;
      this.radix = radix;
      this.nullable = nullable;
      this.charOctetLength = charOctetLength;
      this.ordinalPosition = ordinalPosition;
      this.isNullable = isNullable;
      this.specificName = specificName;
    }
  }

  /** Metadata describing a pseudo column. */
  public static class MetaPseudoColumn {
    public final String tableCat;
    public final String tableSchem;
    @ColumnNoNulls
    public final String tableName;
    @ColumnNoNulls
    public final String columnName;
    public final int dataType;
    public final Integer columnSize;
    public final Integer decimalDigits;
    public final Integer numPrecRadix;
    @ColumnNoNulls
    public final String columnUsage;
    public final String remarks = null;
    public final Integer charOctetLength;
    @ColumnNoNulls
    public final String isNullable;

    public MetaPseudoColumn(
        String tableCat,
        String tableSchem,
        String tableName,
        String columnName,
        int dataType,
        Integer columnSize,
        Integer decimalDigits,
        Integer numPrecRadix,
        String columnUsage,
        Integer charOctetLength,
        String isNullable) {
      this.tableCat = tableCat;
      this.tableSchem = tableSchem;
      this.tableName = tableName;
      this.columnName = columnName;
      this.dataType = dataType;
      this.columnSize = columnSize;
      this.decimalDigits = decimalDigits;
      this.numPrecRadix = numPrecRadix;
      this.columnUsage = columnUsage;
      this.charOctetLength = charOctetLength;
      this.isNullable = isNullable;
    }
  }

  /** Metadata describing a super-table. */
  public static class MetaSuperTable {
    public final String tableCat;
    public final String tableSchem;
    @ColumnNoNulls
    public final String tableName;
    @ColumnNoNulls
    public final String supertableName;

    public MetaSuperTable(
        String tableCat,
        String tableSchem,
        String tableName,
        String supertableName) {
      this.tableCat = tableCat;
      this.tableSchem = tableSchem;
      this.tableName = tableName;
      this.supertableName = supertableName;
    }
  }

  public Map<DatabaseProperty, Object> getDatabaseProperties(ConnectionHandle ch) {
    return Collections.emptyMap();
  }

  public MetaResultSet getTables(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      List<String> typeList) {
    return createEmptyResultSet(MetaTable.class);
  }

  public MetaResultSet getColumns(ConnectionHandle ch, String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern) {
    return createEmptyResultSet(MetaColumn.class);
  }

  public MetaResultSet getSchemas(ConnectionHandle ch, String catalog, Pat schemaPattern) {
    return createEmptyResultSet(MetaSchema.class);
  }

  public MetaResultSet getCatalogs(ConnectionHandle ch) {
    return createEmptyResultSet(MetaCatalog.class);
  }

  public MetaResultSet getTableTypes(ConnectionHandle ch) {
    return createEmptyResultSet(MetaTableType.class);
  }

  public MetaResultSet getProcedures(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern) {
    return createEmptyResultSet(MetaProcedure.class);
  }

  public MetaResultSet getProcedureColumns(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern,
      Pat columnNamePattern) {
    return createEmptyResultSet(MetaProcedureColumn.class);
  }

  public MetaResultSet getColumnPrivileges(ConnectionHandle ch,
      String catalog,
      String schema,
      String table,
      Pat columnNamePattern) {
    return createEmptyResultSet(MetaColumnPrivilege.class);
  }

  public MetaResultSet getTablePrivileges(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern) {
    return createEmptyResultSet(MetaTablePrivilege.class);
  }

  public MetaResultSet getBestRowIdentifier(ConnectionHandle ch,
      String catalog,
      String schema,
      String table,
      int scope,
      boolean nullable) {
    return createEmptyResultSet(MetaBestRowIdentifier.class);
  }

  public MetaResultSet getVersionColumns(ConnectionHandle ch,
      String catalog,
      String schema,
      String table) {
    return createEmptyResultSet(MetaVersionColumn.class);
  }

  public MetaResultSet getPrimaryKeys(ConnectionHandle ch,
      String catalog,
      String schema,
      String table) {
    return createEmptyResultSet(MetaPrimaryKey.class);
  }

  public MetaResultSet getImportedKeys(ConnectionHandle ch,
      String catalog,
      String schema,
      String table) {
    return createEmptyResultSet(MetaImportedKey.class);
  }

  public MetaResultSet getExportedKeys(ConnectionHandle ch,
      String catalog,
      String schema,
      String table) {
    return createEmptyResultSet(MetaExportedKey.class);
  }

  public MetaResultSet getCrossReference(ConnectionHandle ch,
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable) {
    return createEmptyResultSet(MetaCrossReference.class);
  }

  public MetaResultSet getTypeInfo(ConnectionHandle ch) {
    return createEmptyResultSet(MetaTypeInfo.class);
  }

  public MetaResultSet getIndexInfo(ConnectionHandle ch,
      String catalog,
      String schema,
      String table,
      boolean unique,
      boolean approximate) {
    return createEmptyResultSet(MetaIndexInfo.class);
  }

  public MetaResultSet getUDTs(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      int[] types) {
    return createEmptyResultSet(MetaUdt.class);
  }

  public MetaResultSet getSuperTypes(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat typeNamePattern) {
    return createEmptyResultSet(MetaSuperType.class);
  }

  public MetaResultSet getSuperTables(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern) {
    return createEmptyResultSet(MetaSuperTable.class);
  }

  public MetaResultSet getAttributes(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      Pat attributeNamePattern) {
    return createEmptyResultSet(MetaAttribute.class);
  }

  public MetaResultSet getClientInfoProperties(ConnectionHandle ch) {
    return createEmptyResultSet(MetaClientInfoProperty.class);
  }

  public MetaResultSet getFunctions(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat functionNamePattern) {
    return createEmptyResultSet(MetaFunction.class);
  }

  public MetaResultSet getFunctionColumns(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat functionNamePattern,
      Pat columnNamePattern) {
    return createEmptyResultSet(MetaFunctionColumn.class);
  }

  public MetaResultSet getPseudoColumns(ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern) {
    return createEmptyResultSet(MetaPseudoColumn.class);
  }

  @Override public Iterable<Object> createIterable(StatementHandle handle, QueryState state,
      Signature signature, List<TypedValue> parameterValues, Frame firstFrame) {
    if (firstFrame != null && firstFrame.done) {
      return firstFrame.rows;
    }
    AvaticaStatement stmt;
    try {
      stmt = connection.lookupStatement(handle);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return new FetchIterable(stmt, state,
        firstFrame, parameterValues);
  }

  public Frame fetch(AvaticaStatement stmt, List<TypedValue> parameterValues,
      long offset, int fetchMaxRowCount) throws NoSuchStatementException, MissingResultsException {
    return null;
  }

  /** Information about a type. */
  private static class TypeInfo {
    private static Map<Class<?>, TypeInfo> m =
        new HashMap<Class<?>, TypeInfo>();
    static {
      put(boolean.class, Types.BOOLEAN, "BOOLEAN");
      put(Boolean.class, Types.BOOLEAN, "BOOLEAN");
      put(byte.class, Types.TINYINT, "TINYINT");
      put(Byte.class, Types.TINYINT, "TINYINT");
      put(short.class, Types.SMALLINT, "SMALLINT");
      put(Short.class, Types.SMALLINT, "SMALLINT");
      put(int.class, Types.INTEGER, "INTEGER");
      put(Integer.class, Types.INTEGER, "INTEGER");
      put(long.class, Types.BIGINT, "BIGINT");
      put(Long.class, Types.BIGINT, "BIGINT");
      put(float.class, Types.FLOAT, "FLOAT");
      put(Float.class, Types.FLOAT, "FLOAT");
      put(double.class, Types.DOUBLE, "DOUBLE");
      put(Double.class, Types.DOUBLE, "DOUBLE");
      put(String.class, Types.VARCHAR, "VARCHAR");
      put(java.sql.Date.class, Types.DATE, "DATE");
      put(Time.class, Types.TIME, "TIME");
      put(Timestamp.class, Types.TIMESTAMP, "TIMESTAMP");
    }

    private final int sqlType;
    private final String sqlTypeName;

    public TypeInfo(int sqlType, String sqlTypeName) {
      this.sqlType = sqlType;
      this.sqlTypeName = sqlTypeName;
    }

    static void put(Class<?> clazz, int sqlType, String sqlTypeName) {
      m.put(clazz, new TypeInfo(sqlType, sqlTypeName));
    }
  }

  /** Iterator that never returns any elements. */
  private static class EmptyIterator implements Iterator<Object> {
    public static final Iterator<Object> INSTANCE = new EmptyIterator();

    public void remove() {
      throw new UnsupportedOperationException();
    }

    public boolean hasNext() {
      return false;
    }

    public Object next() {
      throw new NoSuchElementException();
    }
  }

  /** Iterable that yields an iterator over rows coming from a sequence of
   * {@link Meta.Frame}s. */
  private class FetchIterable implements Iterable<Object> {
    private final AvaticaStatement stmt;
    private final QueryState state;
    private final Frame firstFrame;
    private final List<TypedValue> parameterValues;

    public FetchIterable(AvaticaStatement stmt, QueryState state, Frame firstFrame,
        List<TypedValue> parameterValues) {
      this.stmt = stmt;
      this.state = state;
      this.firstFrame = firstFrame;
      this.parameterValues = parameterValues;
    }

    public Iterator<Object> iterator() {
      return new FetchIterator(stmt, state, firstFrame, parameterValues);
    }
  }

  /** Iterator over rows coming from a sequence of {@link Meta.Frame}s. */
  private class FetchIterator implements Iterator<Object> {
    private final AvaticaStatement stmt;
    private final QueryState state;
    private Frame frame;
    private Iterator<Object> rows;
    private List<TypedValue> parameterValues;
    private List<TypedValue> originalParameterValues;
    private long currentOffset = 0;

    public FetchIterator(AvaticaStatement stmt, QueryState state, Frame firstFrame,
        List<TypedValue> parameterValues) {
      this.stmt = stmt;
      this.state = state;
      this.parameterValues = parameterValues;
      this.originalParameterValues = parameterValues;
      if (firstFrame == null) {
        frame = Frame.MORE;
        rows = EmptyIterator.INSTANCE;
      } else {
        frame = firstFrame;
        rows = firstFrame.rows.iterator();
      }
      moveNext();
    }

    public void remove() {
      throw new UnsupportedOperationException("remove");
    }

    public boolean hasNext() {
      return rows != null;
    }

    public Object next() {
      if (rows == null) {
        throw new NoSuchElementException();
      }
      final Object o = rows.next();
      currentOffset++;
      moveNext();
      return o;
    }

    private void moveNext() {
      for (;;) {
        if (rows.hasNext()) {
          break;
        }
        if (frame.done) {
          rows = null;
          break;
        }
        try {
          // currentOffset updated after element is read from `rows` iterator
          frame = fetch(stmt.handle, currentOffset, AvaticaStatement.DEFAULT_FETCH_SIZE);
        } catch (NoSuchStatementException e) {
          resetStatement();
          // re-fetch the batch where we left off
          continue;
        } catch (MissingResultsException e) {
          try {
            // We saw the statement, but it didnt' have a resultset initialized. So, reset it.
            if (!stmt.syncResults(state, currentOffset)) {
              // This returned false, so there aren't actually any more results to iterate over
              frame = null;
              rows = null;
              break;
            }
            // syncResults returning true means we need to fetch those results
          } catch (NoSuchStatementException e1) {
            // Tried to reset the result set, but lost the statement, save a loop before retrying.
            resetStatement();
            // Will just loop back around to a MissingResultsException, but w/e recursion
          }
          // Kick back to the top to try to fetch again (in both branches)
          continue;
        }
        parameterValues = null; // don't execute next time
        if (frame == null) {
          rows = null;
          break;
        }
        // It is valid for rows to be empty, so we go around the loop again to
        // check
        rows = frame.rows.iterator();
      }
    }

    private void resetStatement() {
      // If we have to reset the statement, we need to reset the parameterValues too
      parameterValues = originalParameterValues;
      // Defer to the statement to reset itself
      stmt.resetStatement();
    }
  }

  /** Returns whether a list of parameter values has any null elements. */
  public static boolean checkParameterValueHasNull(List<TypedValue> parameterValues) {
    for (TypedValue x : parameterValues) {
      if (x == null) {
        return true;
      }
    }
    return false;
  }
}

// End MetaImpl.java
