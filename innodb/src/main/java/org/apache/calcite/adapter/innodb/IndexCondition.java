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

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import org.apache.commons.collections.CollectionUtils;

import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * Index condition works in the following places.
 * <p>In {@link InnodbFilterTranslator}, it is the index condition
 * to push down according to {@link InnodbFilter} by planner rule.
 * <p>In {@link InnodbTableScan}, it represents a full scan by a
 * primary key or a secondary key.
 * <p>In code generation, it indicates the storage engine which index
 * to use and the associated condition if present.
 */
public class IndexCondition {

  static final IndexCondition EMPTY_CONDITION = new IndexCondition();

  /** Field names per row type. */
  private List<String> fieldNames;
  private String indexName;
  private List<String> indexColumnNames;
  private RelCollation implicitCollation;
  private List<RexNode> pushDownConditions;
  private List<RexNode> remainderConditions;

  private QueryType queryType;
  private List<Object> pointQueryKey;
  private ComparisonOperator rangeQueryLowerOp = ComparisonOperator.NOP;
  private ComparisonOperator rangeQueryUpperOp = ComparisonOperator.NOP;
  private List<Object> rangeQueryLowerKey = ImmutableList.of();
  private List<Object> rangeQueryUpperKey = ImmutableList.of();

  private IndexCondition() {
  }

  static IndexCondition create() {
    return new IndexCondition();
  }

  /**
   * Create a new instance for {@link InnodbFilterTranslator} to build
   * index condition which can be pushed down.
   */
  IndexCondition(
      List<String> fieldNames,
      String indexName,
      List<String> indexColumnNames,
      List<RexNode> pushDownConditions,
      List<RexNode> remainderConditions) {
    this.fieldNames = fieldNames;
    this.indexName = indexName;
    this.indexColumnNames = indexColumnNames;
    this.pushDownConditions = pushDownConditions;
    this.remainderConditions = remainderConditions;
    this.implicitCollation = getImplicitCollation(indexColumnNames);
  }

  /**
   * Create a new instance for code generation to build query parameters
   * for underlying storage engine <code>Innodb-java-reader</code>.
   */
  public static IndexCondition create(
      String indexName,
      QueryType queryType,
      List<Object> pointQueryKey,
      ComparisonOperator rangeQueryLowerOp,
      ComparisonOperator rangeQueryUpperOp,
      List<Object> rangeQueryLowerKey,
      List<Object> rangeQueryUpperKey) {
    IndexCondition condition = new IndexCondition();
    condition.setIndexName(indexName);
    condition.setQueryType(queryType);
    condition.setPointQueryKey(pointQueryKey);
    condition.setRangeQueryLowerOp(rangeQueryLowerOp);
    condition.setRangeQueryUpperOp(rangeQueryUpperOp);
    condition.setRangeQueryLowerKey(rangeQueryLowerKey);
    condition.setRangeQueryUpperKey(rangeQueryUpperKey);
    return condition;
  }

  /**
   * If there are any push down conditions, return true.
   */
  boolean canPushDown() {
    return CollectionUtils.isNotEmpty(pushDownConditions);
  }

  public RelCollation getImplicitCollation() {
    if (implicitCollation == null) {
      assert indexColumnNames != null;
      implicitCollation = getImplicitCollation(indexColumnNames);
    }
    return implicitCollation;
  }

  /**
   * Infer the implicit correlation from the index.
   *
   * @param indexColumnNames index column names
   * @return the collation of the filtered results
   */
  public RelCollation getImplicitCollation(List<String> indexColumnNames) {
    checkState(fieldNames != null, "field names cannot be null");
    List<RelFieldCollation> keyCollations = new ArrayList<>(indexColumnNames.size());
    for (String keyColumnName : indexColumnNames) {
      int fieldIndex = fieldNames.indexOf(keyColumnName);
      keyCollations.add(new RelFieldCollation(fieldIndex, RelFieldCollation.Direction.ASCENDING));
    }
    return RelCollations.of(keyCollations);
  }

  public IndexCondition setFieldNames(List<String> fieldNames) {
    this.fieldNames = fieldNames;
    return this;
  }

  public String getIndexName() {
    return indexName;
  }

  public IndexCondition setIndexName(String indexName) {
    this.indexName = indexName;
    return this;
  }

  public IndexCondition setIndexColumnNames(List<String> indexColumnNames) {
    this.indexColumnNames = indexColumnNames;
    return this;
  }

  public List<RexNode> getPushDownConditions() {
    return pushDownConditions == null ? ImmutableList.of() : pushDownConditions;
  }

  public List<RexNode> getRemainderConditions() {
    return remainderConditions == null ? ImmutableList.of() : remainderConditions;
  }

  public QueryType getQueryType() {
    return queryType;
  }

  public IndexCondition setQueryType(QueryType queryType) {
    this.queryType = queryType;
    return this;
  }

  public List<Object> getPointQueryKey() {
    return pointQueryKey;
  }

  public IndexCondition setPointQueryKey(List<Object> pointQueryKey) {
    this.pointQueryKey = pointQueryKey;
    return this;
  }

  public ComparisonOperator getRangeQueryLowerOp() {
    return rangeQueryLowerOp;
  }

  public IndexCondition setRangeQueryLowerOp(ComparisonOperator rangeQueryLowerOp) {
    this.rangeQueryLowerOp = rangeQueryLowerOp;
    return this;
  }

  public ComparisonOperator getRangeQueryUpperOp() {
    return rangeQueryUpperOp;
  }

  public IndexCondition setRangeQueryUpperOp(ComparisonOperator rangeQueryUpperOp) {
    this.rangeQueryUpperOp = rangeQueryUpperOp;
    return this;
  }

  public List<Object> getRangeQueryLowerKey() {
    return rangeQueryLowerKey;
  }

  public IndexCondition setRangeQueryLowerKey(List<Object> rangeQueryLowerKey) {
    this.rangeQueryLowerKey = rangeQueryLowerKey;
    return this;
  }

  public List<Object> getRangeQueryUpperKey() {
    return rangeQueryUpperKey;
  }

  public IndexCondition setRangeQueryUpperKey(List<Object> rangeQueryUpperKey) {
    this.rangeQueryUpperKey = rangeQueryUpperKey;
    return this;
  }

  public boolean nameMatch(String name) {
    return name != null && name.equalsIgnoreCase(indexName);
  }

  @Override public String toString() {
    final StringBuilder builder = new StringBuilder("(");
    builder.append(queryType).append(", index=").append(indexName);
    if (queryType == QueryType.PK_POINT_QUERY
        || queryType == QueryType.SK_POINT_QUERY) {
      checkState(pointQueryKey.size() == indexColumnNames.size());
      append(builder, indexColumnNames, pointQueryKey, "=");
    } else {
      if (CollectionUtils.isNotEmpty(rangeQueryLowerKey)) {
        append(builder, indexColumnNames, rangeQueryLowerKey, rangeQueryLowerOp.value());
      }
      if (CollectionUtils.isNotEmpty(rangeQueryUpperKey)) {
        append(builder, indexColumnNames, rangeQueryUpperKey, rangeQueryUpperOp.value());
      }
    }
    builder.append(")");
    return builder.toString();
  }

  private void append(StringBuilder builder, List<String> keyColumnNames,
      List<Object> key, String op) {
    builder.append(", ");
    for (Pair<String, Object> value : Pair.zip(keyColumnNames, key)) {
      builder.append(value.getKey());
      builder.append(op);
      builder.append(value.getValue());
      builder.append(",");
    }
    builder.deleteCharAt(builder.length() - 1);
  }
}
