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

import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;

import static java.util.Objects.requireNonNull;

/**
 * Index condition.
 *
 * <p>Works in the following places:
 *
 * <ul>
 * <li>In {@link InnodbFilterTranslator}, it is the index condition
 * to push down according to {@link InnodbFilter} by planner rule.
 *
 * <li>In {@link InnodbTableScan}, it represents a full scan by a
 * primary key or a secondary key.
 *
 * <li>In code generation, it indicates the storage engine which index
 * to use and the associated condition if present.
 * </ul>
 */
public class IndexCondition {

  static final IndexCondition EMPTY_CONDITION =
      create(null, null, null, ComparisonOperator.NOP, ComparisonOperator.NOP,
          ImmutableList.of(), ImmutableList.of());

  /** Field names per row type. */
  private final List<String> fieldNames;
  private final String indexName;
  private final List<String> indexColumnNames;
  private final RelCollation implicitCollation;
  private final List<RexNode> pushDownConditions;
  private final List<RexNode> remainderConditions;

  private final QueryType queryType;
  private final List<Object> pointQueryKey;
  private final ComparisonOperator rangeQueryLowerOp;
  private final ComparisonOperator rangeQueryUpperOp;
  private final List<Object> rangeQueryLowerKey;
  private final List<Object> rangeQueryUpperKey;

  /** Constructor that assigns all fields. All other constructors call this. */
  private IndexCondition(
      List<String> fieldNames,
      String indexName,
      List<String> indexColumnNames,
      RelCollation implicitCollation,
      List<RexNode> pushDownConditions,
      List<RexNode> remainderConditions,
      QueryType queryType,
      List<Object> pointQueryKey,
      ComparisonOperator rangeQueryLowerOp,
      ComparisonOperator rangeQueryUpperOp,
      List<Object> rangeQueryLowerKey,
      List<Object> rangeQueryUpperKey) {
    this.fieldNames = fieldNames;
    this.indexName = indexName;
    this.indexColumnNames = indexColumnNames;
    this.implicitCollation =
        implicitCollation != null ? implicitCollation
            : deduceImplicitCollation(fieldNames, indexColumnNames);
    this.pushDownConditions =
        pushDownConditions == null ? ImmutableList.of()
            : ImmutableList.copyOf(pushDownConditions);
    this.remainderConditions =
        remainderConditions == null ? ImmutableList.of()
            : ImmutableList.copyOf(remainderConditions);
    this.queryType = queryType;
    this.pointQueryKey =
        pointQueryKey == null ? ImmutableList.of()
            : ImmutableList.copyOf(pointQueryKey);
    this.rangeQueryLowerOp = requireNonNull(rangeQueryLowerOp, "rangeQueryLowerOp");
    this.rangeQueryUpperOp = requireNonNull(rangeQueryUpperOp, "rangeQueryUpperOp");
    this.rangeQueryLowerKey = ImmutableList.copyOf(rangeQueryLowerKey);
    this.rangeQueryUpperKey = ImmutableList.copyOf(rangeQueryUpperKey);
  }

  static IndexCondition create(
      List<String> fieldNames,
      String indexName,
      List<String> indexColumnNames,
      QueryType queryType) {
    return new IndexCondition(fieldNames, indexName, indexColumnNames, null,
        null, null, queryType, null, ComparisonOperator.NOP,
        ComparisonOperator.NOP, ImmutableList.of(), ImmutableList.of());
  }

  /**
   * Creates a new instance for {@link InnodbFilterTranslator} to build
   * index condition which can be pushed down.
   */
  static IndexCondition create(
      List<String> fieldNames,
      String indexName,
      List<String> indexColumnNames,
      List<RexNode> pushDownConditions,
      List<RexNode> remainderConditions) {
    return new IndexCondition(fieldNames, indexName, indexColumnNames, null,
        pushDownConditions, remainderConditions, null, null,
        ComparisonOperator.NOP, ComparisonOperator.NOP, ImmutableList.of(),
        ImmutableList.of());
  }

  /**
   * Creates a new instance for code generation to build query parameters
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
    return new IndexCondition(ImmutableList.of(), indexName, ImmutableList.of(),
        null, null, null, queryType, pointQueryKey, rangeQueryLowerOp,
        rangeQueryUpperOp, rangeQueryLowerKey, rangeQueryUpperKey);
  }

  /** Returns whether there are any push down conditions. */
  boolean canPushDown() {
    return !pushDownConditions.isEmpty();
  }

  public RelCollation getImplicitCollation() {
    return implicitCollation;
  }

  /**
   * Infers the implicit correlation from the index.
   *
   * @param indexColumnNames index column names
   * @return the collation of the filtered results
   */
  private static RelCollation deduceImplicitCollation(List<String> fieldNames,
      List<String> indexColumnNames) {
    requireNonNull(fieldNames, "field names must not be null");
    List<RelFieldCollation> keyCollations = new ArrayList<>(indexColumnNames.size());
    for (String keyColumnName : indexColumnNames) {
      int fieldIndex = fieldNames.indexOf(keyColumnName);
      keyCollations.add(
          new RelFieldCollation(fieldIndex, RelFieldCollation.Direction.ASCENDING));
    }
    return RelCollations.of(keyCollations);
  }

  public IndexCondition withFieldNames(List<String> fieldNames) {
    if (Objects.equals(fieldNames, this.fieldNames)) {
      return this;
    }
    return new IndexCondition(fieldNames, indexName, indexColumnNames,
        implicitCollation, pushDownConditions, remainderConditions,
        queryType, pointQueryKey, rangeQueryLowerOp, rangeQueryUpperOp,
        rangeQueryLowerKey, rangeQueryUpperKey);
  }

  public String getIndexName() {
    return indexName;
  }

  public IndexCondition withIndexName(String indexName) {
    if (Objects.equals(indexName, this.indexName)) {
      return this;
    }
    return new IndexCondition(fieldNames, indexName, indexColumnNames,
        implicitCollation, pushDownConditions, remainderConditions,
        queryType, pointQueryKey, rangeQueryLowerOp, rangeQueryUpperOp,
        rangeQueryLowerKey, rangeQueryUpperKey);
  }

  public IndexCondition withIndexColumnNames(List<String> indexColumnNames) {
    if (Objects.equals(indexColumnNames, this.indexColumnNames)) {
      return this;
    }
    return new IndexCondition(fieldNames, indexName, indexColumnNames,
        implicitCollation, pushDownConditions, remainderConditions,
        queryType, pointQueryKey, rangeQueryLowerOp, rangeQueryUpperOp,
        rangeQueryLowerKey, rangeQueryUpperKey);
  }

  public List<RexNode> getPushDownConditions() {
    return pushDownConditions;
  }

  public IndexCondition withPushDownConditions(List<RexNode> pushDownConditions) {
    if (Objects.equals(pushDownConditions, this.pushDownConditions)) {
      return this;
    }
    return new IndexCondition(fieldNames, indexName, indexColumnNames,
        implicitCollation, pushDownConditions, remainderConditions,
        queryType, pointQueryKey, rangeQueryLowerOp, rangeQueryUpperOp,
        rangeQueryLowerKey, rangeQueryUpperKey);
  }

  public List<RexNode> getRemainderConditions() {
    return remainderConditions;
  }

  public IndexCondition withRemainderConditions(List<RexNode> remainderConditions) {
    if (Objects.equals(remainderConditions, this.remainderConditions)) {
      return this;
    }
    return new IndexCondition(fieldNames, indexName, indexColumnNames,
        implicitCollation, pushDownConditions, remainderConditions,
        queryType, pointQueryKey, rangeQueryLowerOp, rangeQueryUpperOp,
        rangeQueryLowerKey, rangeQueryUpperKey);
  }

  public QueryType getQueryType() {
    return queryType;
  }

  public IndexCondition withQueryType(QueryType queryType) {
    if (queryType == this.queryType) {
      return this;
    }
    return new IndexCondition(fieldNames, indexName, indexColumnNames,
        implicitCollation, pushDownConditions, remainderConditions,
        queryType, pointQueryKey, rangeQueryLowerOp, rangeQueryUpperOp,
        rangeQueryLowerKey, rangeQueryUpperKey);
  }

  public List<Object> getPointQueryKey() {
    return pointQueryKey;
  }

  public IndexCondition withPointQueryKey(List<Object> pointQueryKey) {
    if (pointQueryKey == this.pointQueryKey) {
      return this;
    }
    return new IndexCondition(fieldNames, indexName, indexColumnNames,
        implicitCollation, pushDownConditions, remainderConditions,
        queryType, pointQueryKey, rangeQueryLowerOp, rangeQueryUpperOp,
        rangeQueryLowerKey, rangeQueryUpperKey);
  }

  public ComparisonOperator getRangeQueryLowerOp() {
    return rangeQueryLowerOp;
  }

  public IndexCondition withRangeQueryLowerOp(ComparisonOperator rangeQueryLowerOp) {
    if (rangeQueryLowerOp == this.rangeQueryLowerOp) {
      return this;
    }
    return new IndexCondition(fieldNames, indexName, indexColumnNames,
        implicitCollation, pushDownConditions, remainderConditions,
        queryType, pointQueryKey, rangeQueryLowerOp, rangeQueryUpperOp,
        rangeQueryLowerKey, rangeQueryUpperKey);
  }

  public ComparisonOperator getRangeQueryUpperOp() {
    return rangeQueryUpperOp;
  }

  public IndexCondition withRangeQueryUpperOp(ComparisonOperator rangeQueryUpperOp) {
    if (rangeQueryUpperOp == this.rangeQueryUpperOp) {
      return this;
    }
    return new IndexCondition(fieldNames, indexName, indexColumnNames,
        implicitCollation, pushDownConditions, remainderConditions,
        queryType, pointQueryKey, rangeQueryLowerOp, rangeQueryUpperOp,
        rangeQueryLowerKey, rangeQueryUpperKey);
  }

  public List<Object> getRangeQueryLowerKey() {
    return rangeQueryLowerKey;
  }

  public IndexCondition withRangeQueryLowerKey(List<Object> rangeQueryLowerKey) {
    if (rangeQueryLowerKey == this.rangeQueryLowerKey) {
      return this;
    }
    return new IndexCondition(fieldNames, indexName, indexColumnNames,
        implicitCollation, pushDownConditions, remainderConditions,
        queryType, pointQueryKey, rangeQueryLowerOp, rangeQueryUpperOp,
        rangeQueryLowerKey, rangeQueryUpperKey);
  }

  public List<Object> getRangeQueryUpperKey() {
    return rangeQueryUpperKey;
  }

  public IndexCondition withRangeQueryUpperKey(List<Object> rangeQueryUpperKey) {
    if (rangeQueryUpperKey == this.rangeQueryUpperKey) {
      return this;
    }
    return new IndexCondition(fieldNames, indexName, indexColumnNames,
        implicitCollation, pushDownConditions, remainderConditions,
        queryType, pointQueryKey, rangeQueryLowerOp, rangeQueryUpperOp,
        rangeQueryLowerKey, rangeQueryUpperKey);
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
      if (!rangeQueryLowerKey.isEmpty()) {
        append(builder, indexColumnNames, rangeQueryLowerKey, rangeQueryLowerOp.value());
      }
      if (!rangeQueryUpperKey.isEmpty()) {
        append(builder, indexColumnNames, rangeQueryUpperKey, rangeQueryUpperOp.value());
      }
    }
    builder.append(")");
    return builder.toString();
  }

  private static void append(StringBuilder builder, List<String> keyColumnNames,
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
