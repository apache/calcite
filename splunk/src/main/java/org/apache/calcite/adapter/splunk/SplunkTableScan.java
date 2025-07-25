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
package org.apache.calcite.adapter.splunk;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Relational expression representing a scan of Splunk using JSON output.
 * Much simpler than CSV approach - no complex field mapping needed.
 */
public class SplunkTableScan
    extends TableScan
    implements EnumerableRel {
  private static final Logger LOGGER = LoggerFactory.getLogger(SplunkTableScan.class);

  final SplunkTable splunkTable;
  final String search;
  final String earliest;
  final String latest;
  final List<String> fieldList;

  protected SplunkTableScan(
      RelOptCluster cluster,
      RelOptTable table,
      SplunkTable splunkTable,
      String search,
      String earliest,
      String latest,
      List<String> fieldList) {
    super(
        cluster,
        cluster.traitSetOf(EnumerableConvention.INSTANCE),
        ImmutableList.of(),
        table);
    this.splunkTable = requireNonNull(splunkTable, "splunkTable");
    this.search = requireNonNull(search, "search");
    this.earliest = earliest;
    this.latest = latest;
    this.fieldList = fieldList;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("table", table.getQualifiedName())
        .item("search", search)
        .item("earliest", earliest)
        .item("latest", latest)
        .item("fieldList", fieldList)
        .item("fieldMapping", splunkTable.getFieldMapping());
  }

  @Override public void register(RelOptPlanner planner) {
    planner.addRule(SplunkPushDownRule.FILTER);
    planner.addRule(SplunkPushDownRule.FILTER_ON_PROJECT);
    planner.addRule(SplunkPushDownRule.PROJECT);
    planner.addRule(SplunkPushDownRule.PROJECT_ON_FILTER);
  }

  @Override public RelDataType deriveRowType() {
    final RelDataTypeFactory.Builder builder =
        getCluster().getTypeFactory().builder();
    for (String field : fieldList) {
      // REVIEW: is case-sensitive match what we want here?
      RelDataTypeField dataTypeField = table.getRowType().getField(field, true, false);
      if (dataTypeField != null) {
        builder.add(dataTypeField);
      } else {
        // Handle missing field - add as VARCHAR type
        builder.add(field, getCluster().getTypeFactory().createJavaType(String.class));
      }
    }
    return builder.build();
  }

  public SplunkTable getSplunkTable() {
    return splunkTable;
  }

  public String getSearch() {
    return search;
  }

  public String getEarliest() {
    return earliest;
  }

  public String getLatest() {
    return latest;
  }

  public List<String> getFieldNames() {
    return fieldList;
  }

  /**
   * Creates a new SplunkTableScan with updated search parameters.
   * This is used by push-down rules to modify the search.
   */
  public SplunkTableScan withSearchParameters(String newSearch, String newEarliest,
      String newLatest, List<String> newFieldList) {
    if (search.equals(newSearch) && earliest.equals(newEarliest) &&
        latest.equals(newLatest) && fieldList.equals(newFieldList)) {
      return this;
    }
    return new SplunkTableScan(getCluster(), table, splunkTable,
        newSearch, newEarliest, newLatest, newFieldList);
  }

  // Simple method reference - only one createQuery method needed with JSON
  private static final Method METHOD =
      Types.lookupMethod(
          SplunkTable.SplunkTableQueryable.class,
          "createQuery",
          String.class,
          String.class,
          String.class,
          List.class);

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // With JSON, we can pass schema field names directly - much simpler!
    Map<String, Object> map = ImmutableMap.<String, Object>builder()
        .put("search", search)
        .put("earliest", Util.first(earliest, ""))
        .put("latest", Util.first(latest, ""))
        .put("fieldList", fieldList)                    // Schema field names
        .put("fieldMapping", splunkTable.getFieldMapping())
        .build();

    if (CalciteSystemProperty.DEBUG.value()) {
      LOGGER.debug("Splunk JSON Mode: {}", map);
    }
    Hook.QUERY_PLAN.run(map);

    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.preferCustom());
    final BlockBuilder builder = new BlockBuilder();

    // Simple call - just pass schema field names
    return implementor.result(
        physType,
        builder.append(
                Expressions.call(
                    table.getExpression(SplunkTable.SplunkTableQueryable.class),
                    METHOD,
                    Expressions.constant(search),
                    Expressions.constant(earliest),
                    Expressions.constant(latest),
                    constantStringList(fieldList)))      // Schema field names only
            .toBlock());
  }

  private static Expression constantStringList(final List<String> strings) {
    return Expressions.call(
        Arrays.class,
        "asList",
        Expressions.newArrayInit(
            Object.class,
            new AbstractList<Expression>() {
              @Override public Expression get(int index) {
                return Expressions.constant(strings.get(index));
              }

              @Override public int size() {
                return strings.size();
              }
            }));
  }
}
