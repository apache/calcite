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
package org.apache.calcite.adapter.geode.rel;

import org.apache.calcite.adapter.geode.util.GeodeUtils;
import org.apache.calcite.adapter.geode.util.JavaTypeFactoryExtImpl;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Table based on a Geode Region
 */
public class GeodeTable extends AbstractQueryableTable implements TranslatableTable {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeodeTable.class.getName());

  private final String regionName;
  private final RelDataType rowType;

  GeodeTable(Region<?, ?> region) {
    super(Object[].class);
    this.regionName = region.getName();
    this.rowType = GeodeUtils.autodetectRelTypeFromRegion(region);
  }

  public String toString() {
    return "GeodeTable {" + regionName + "}";
  }

  /**
   * Executes an OQL query on the underlying table.
   *
   * <p>Called by the {@link GeodeQueryable} which in turn is
   * called via the generated code.
   *
   * @param clientCache Geode client cache
   * @param fields      List of fields to project
   * @param predicates  A list of predicates which should be used in the query
   * @return Enumerator of results
   */
  public Enumerable<Object> query(final GemFireCache clientCache,
      final List<Map.Entry<String, Class>> fields,
      final List<Map.Entry<String, String>> selectFields,
      final List<Map.Entry<String, String>> aggregateFunctions,
      final List<String> groupByFields,
      List<String> predicates,
      List<String> orderByFields,
      Long limit) {

    final RelDataTypeFactory typeFactory = new JavaTypeFactoryExtImpl();
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();

    for (Map.Entry<String, Class> field : fields) {
      SqlTypeName typeName = typeFactory.createJavaType(field.getValue()).getSqlTypeName();
      RelDataType type;
      if (typeName == SqlTypeName.ARRAY) {
        type = typeFactory.createArrayType(
            typeFactory.createSqlType(SqlTypeName.ANY),
            -1);
      } else if (typeName == SqlTypeName.MULTISET) {
        type = typeFactory.createMultisetType(
            typeFactory.createSqlType(SqlTypeName.ANY),
            -1);
      } else {
        type = typeFactory.createSqlType(typeName);
      }
      fieldInfo.add(field.getKey(), type).nullable(true);
    }

    final RelProtoDataType resultRowType = RelDataTypeImpl.proto(fieldInfo.build());

    ImmutableMap<String, String> aggFuncMap = ImmutableMap.of();
    if (!aggregateFunctions.isEmpty()) {
      ImmutableMap.Builder<String, String> aggFuncMapBuilder = ImmutableMap.builder();
      for (Map.Entry<String, String> e : aggregateFunctions) {
        aggFuncMapBuilder.put(e.getKey(), e.getValue());
      }
      aggFuncMap = aggFuncMapBuilder.build();
    }

    // Construct the list of fields to project
    Builder<String> selectBuilder = ImmutableList.builder();
    if (!groupByFields.isEmpty()) {
      // manually add GROUP BY to select clause (GeodeProjection was not visited)
      for (String groupByField : groupByFields) {
        selectBuilder.add(groupByField + " AS " + groupByField);
      }

      if (!aggFuncMap.isEmpty()) {
        for (Map.Entry<String, String> e : aggFuncMap.entrySet()) {
          selectBuilder.add(e.getValue() + " AS " + e.getKey());
        }
      }
    } else {
      if (selectFields.isEmpty()) {
        if (!aggFuncMap.isEmpty()) {
          for (Map.Entry<String, String> e : aggFuncMap.entrySet()) {
            selectBuilder.add(e.getValue() + " AS " + e.getKey());
          }
        } else {
          selectBuilder.add("*");
        }
      } else {
        if (!aggFuncMap.isEmpty()) {
          for (Map.Entry<String, String> e : aggFuncMap.entrySet()) {
            selectBuilder.add(e.getValue() + " AS " + e.getKey());
          }
        } else {
          for (Map.Entry<String, String> field : selectFields) {
            selectBuilder.add(field.getKey() + " AS " + field.getValue());
          }
        }
      }
    }

    final String oqlSelectStatement = Util.toString(selectBuilder.build(), "", ", ", "");

    // Combine all predicates conjunctively
    String whereClause = "";
    if (!predicates.isEmpty()) {
      whereClause = " WHERE ";
      whereClause += Util.toString(predicates, "", " AND ", "");
    }

    // Build and issue the query and return an Enumerator over the results
    StringBuilder queryBuilder = new StringBuilder("SELECT ");
    queryBuilder.append(oqlSelectStatement);
    queryBuilder.append(" FROM /" + regionName);
    queryBuilder.append(whereClause);

    if (!groupByFields.isEmpty()) {
      queryBuilder.append(Util.toString(groupByFields, " GROUP BY ", ", ", ""));
    }

    if (!orderByFields.isEmpty()) {
      queryBuilder.append(Util.toString(orderByFields, " ORDER BY ", ", ", ""));
    }
    if (limit != null) {
      queryBuilder.append(" LIMIT " + limit);
    }

    final String oqlQuery = queryBuilder.toString();

    Hook.QUERY_PLAN.run(oqlQuery);
    LOGGER.info("OQL: " + oqlQuery);

    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        final QueryService queryService = clientCache.getQueryService();
        try {
          SelectResults results = (SelectResults) queryService.newQuery(oqlQuery).execute();
          return new GeodeEnumerator(results, resultRowType);
        } catch (Exception e) {
          String message = String.format(Locale.ROOT, "Failed to execute query [%s] on %s",
              oqlQuery, clientCache.getName());
          throw new RuntimeException(message, e);
        }
      }
    };
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new GeodeQueryable<>(queryProvider, schema, this, tableName);
  }

  @Override public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {

    final RelOptCluster cluster = context.getCluster();
    return new GeodeTableScan(cluster, cluster.traitSetOf(GeodeRel.CONVENTION),
        relOptTable, this, null);
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return rowType;
  }

  /**
   * Implementation of {@link Queryable} based on a {@link GeodeTable}.
   *
   * @param <T> type
   */
  public static class GeodeQueryable<T> extends AbstractTableQueryable<T> {

    public GeodeQueryable(QueryProvider queryProvider, SchemaPlus schema,
        GeodeTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    // tzolov: this should never be called for queryable tables???
    public Enumerator<T> enumerator() {
      throw new UnsupportedOperationException("Enumerator on Queryable should never be called");
    }

    private GeodeTable getTable() {
      return (GeodeTable) table;
    }

    private GemFireCache getClientCache() {
      return schema.unwrap(GeodeSchema.class).cache;
    }

    /**
     * Called via code-generation.
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> query(
        List<Map.Entry<String, Class>> fields,
        List<Map.Entry<String, String>> selectFields,
        List<Map.Entry<String, String>> aggregateFunctions,
        List<String> groupByFields,
        List<String> predicates,
        List<String> order,
        Long limit) {
      return getTable().query(getClientCache(), fields, selectFields,
          aggregateFunctions, groupByFields, predicates, order, limit);
    }
  }
}

// End GeodeTable.java
