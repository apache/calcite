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
package org.apache.calcite.adapter.innodb;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import com.alibaba.innodb.java.reader.Constants;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.TableReaderFactory;
import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.KeyMeta;
import com.alibaba.innodb.java.reader.schema.TableDef;
import com.alibaba.innodb.java.reader.service.impl.RecordIterator;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Table based on an InnoDB data file.
 */
public class InnodbTable extends AbstractQueryableTable
    implements TranslatableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(InnodbTable.class);

  private final InnodbSchema schema;
  private final String tableName;
  private final Supplier<RelProtoDataType> protoRowTypeSupplier =
      Suppliers.memoize(this::supplyProto);
  private final Supplier<TableDef> tableDefSupplier =
      Suppliers.memoize(this::supplyTableDef);

  public InnodbTable(InnodbSchema schema, String tableName) {
    super(Object[].class);
    this.schema = schema;
    this.tableName = tableName;
  }

  @Override public String toString() {
    return "InnodbTable {" + tableName + "}";
  }

  private RelProtoDataType supplyProto() {
    return schema.getRelDataType(tableName);
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return protoRowTypeSupplier.get().apply(typeFactory);
  }

  public TableDef getTableDef() {
    return tableDefSupplier.get();
  }

  private TableDef supplyTableDef() {
    return schema.getTableDef(tableName);
  }

  /**
   * Get index name set.
   *
   * @return set of index names
   */
  public Set<String> getIndexesNameSet() {
    return ImmutableSet.<String>builder()
        .add(Constants.PRIMARY_KEY_NAME)
        .addAll(getTableDef().getSecondaryKeyMetaList().stream()
            .map(KeyMeta::getName).collect(Collectors.toList()))
        .build();
  }

  public Enumerable<Object> query(final TableReaderFactory tableReaderFactory) {
    return query(tableReaderFactory, ImmutableList.of(), ImmutableList.of(),
        IndexCondition.EMPTY_CONDITION, true);
  }

  /**
   * Executes a query on the underlying InnoDB table.
   *
   * @param tableReaderFactory InnoDB Java table reader factory
   * @param fields             list of fields
   * @param selectFields       list of fields to project
   * @param condition          push down index condition
   * @param ascOrder           if scan ordering is ascending
   * @return Enumerator of results
   */
  public Enumerable<Object> query(
      final TableReaderFactory tableReaderFactory,
      final List<Map.Entry<String, Class>> fields,
      final List<Map.Entry<String, String>> selectFields,
      final IndexCondition condition,
      final Boolean ascOrder) {
    final QueryType queryType = condition.getQueryType();
    final List<Object> pointQueryKey = condition.getPointQueryKey();
    final ComparisonOperator rangeQueryLowerOp = condition.getRangeQueryLowerOp();
    final List<Object> rangeQueryLowerKey = condition.getRangeQueryLowerKey();
    final ComparisonOperator rangeQueryUpperOp = condition.getRangeQueryUpperOp();
    final List<Object> rangeQueryUpperKey = condition.getRangeQueryUpperKey();
    final String indexName = condition.getIndexName();

    // Build the type of the resulting row based on the provided fields
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    final RelDataType rowType = getRowType(typeFactory);

    Function1<String, Void> addField = fieldName -> {
      final RelDataTypeField field =
          requireNonNull(rowType.getField(fieldName, true, false));
      RelDataType relDataType = field.getType();
      fieldInfo.add(fieldName, relDataType).nullable(relDataType.isNullable());
      return null;
    };

    List<String> selectedColumnNames = new ArrayList<>(selectFields.size());
    if (selectFields.isEmpty()) {
      for (Map.Entry<String, Class> field : fields) {
        addField.apply(field.getKey());
      }
    } else {
      for (Map.Entry<String, String> field : selectFields) {
        addField.apply(field.getKey());
        selectedColumnNames.add(field.getKey());
      }
    }

    final RelProtoDataType resultRowType = RelDataTypeImpl.proto(fieldInfo.build());

    TableReader tableReader = tableReaderFactory.createTableReader(tableName);
    tableReader.open();
    return new AbstractEnumerable<Object>() {
      @Override public Enumerator<Object> enumerator() {
        Iterator<GenericRecord> resultIterator;
        LOGGER.debug("Create query iterator, queryType={}, indexName={}, "
                + "pointQueryKey={}, projection={}, rangeQueryKey={}{} AND {}{}, "
                + "ascOrder={}", queryType, indexName, pointQueryKey,
            selectedColumnNames, rangeQueryLowerKey, rangeQueryLowerOp,
            rangeQueryUpperKey, rangeQueryUpperOp, ascOrder);
        switch (queryType) {
          case PK_POINT_QUERY:
            resultIterator =
                RecordIterator.create(tableReader
                    .queryByPrimaryKey(pointQueryKey, selectedColumnNames));
            break;
          case PK_RANGE_QUERY:
            resultIterator =
                tableReader.getRangeQueryIterator(rangeQueryLowerKey,
                    rangeQueryLowerOp, rangeQueryUpperKey, rangeQueryUpperOp,
                    selectedColumnNames, ascOrder);
            break;
          case SK_POINT_QUERY:
            resultIterator =
                tableReader.getRecordIteratorBySk(indexName, pointQueryKey,
                    ComparisonOperator.GTE, pointQueryKey,
                    ComparisonOperator.LTE, selectedColumnNames, ascOrder);
            break;
          case SK_RANGE_QUERY:
          case SK_FULL_SCAN:
            resultIterator =
                tableReader.getRecordIteratorBySk(indexName, rangeQueryLowerKey,
                    rangeQueryLowerOp, rangeQueryUpperKey, rangeQueryUpperOp,
                    selectedColumnNames, ascOrder);
            break;
          case PK_FULL_SCAN:
            resultIterator =
                tableReader.getQueryAllIterator(selectedColumnNames, ascOrder);
            break;
          default:
            throw new AssertionError("query type is invalid");
        }

        RelDataType rowType = resultRowType.apply(typeFactory);
        return new InnodbEnumerator(resultIterator, rowType) {
          @Override public void close() {
            super.close();
            tableReader.close();
          }
        };
      }
    };
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new InnodbQueryable<>(queryProvider, schema, this, tableName);
  }

  @Override public RelNode toRel(RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new InnodbTableScan(cluster, cluster.traitSetOf(InnodbRel.CONVENTION),
        relOptTable, this, null, context.getTableHints());
  }

  /**
   * Implementation of {@link org.apache.calcite.linq4j.Queryable} based on
   * a {@link org.apache.calcite.adapter.innodb.InnodbTable}.
   *
   * @param <T> element type
   */
  public static class InnodbQueryable<T> extends AbstractTableQueryable<T> {
    public InnodbQueryable(QueryProvider queryProvider, SchemaPlus schema,
        InnodbTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    @Override public Enumerator<T> enumerator() {
      //noinspection unchecked
      final Enumerable<T> enumerable =
          (Enumerable<T>) getTable().query(getTableReaderFactory());
      return enumerable.enumerator();
    }

    private InnodbTable getTable() {
      return (InnodbTable) table;
    }

    private TableReaderFactory getTableReaderFactory() {
      final InnodbSchema innodbSchema =
          requireNonNull(schema.unwrap(InnodbSchema.class));
      return innodbSchema.tableReaderFactory;
    }

    /**
     * Called via code-generation.
     *
     * @see org.apache.calcite.adapter.innodb.InnodbMethod#INNODB_QUERYABLE_QUERY
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> query(List<Map.Entry<String, Class>> fields,
        List<Map.Entry<String, String>> selectFields,
        IndexCondition condition, Boolean ascOrder) {
      return getTable().query(getTableReaderFactory(), fields, selectFields,
          condition, ascOrder);
    }
  }
}
