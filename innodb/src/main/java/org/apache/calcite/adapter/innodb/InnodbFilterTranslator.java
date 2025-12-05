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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.schema.KeyMeta;
import com.alibaba.innodb.java.reader.schema.TableDef;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Translates {@link RexNode} expressions into {@link IndexCondition}
 * which might be pushed down to an InnoDB data source.
 */
class InnodbFilterTranslator {
  private final RexBuilder rexBuilder;
  /** Field names per row type. */
  private final List<String> fieldNames;
  /** Primary key metadata. */
  private final KeyMeta pkMeta;
  /** Secondary key metadata. */
  private final List<KeyMeta> skMetaList;
  /** If not null, force to use one specific index from hint. */
  private final @Nullable String forceIndexName;

  InnodbFilterTranslator(RexBuilder rexBuilder, RelDataType rowType,
      TableDef tableDef, @Nullable String forceIndexName) {
    this.rexBuilder = rexBuilder;
    this.fieldNames = InnodbRules.innodbFieldNames(rowType);
    this.pkMeta = tableDef.getPrimaryKeyMeta();
    this.skMetaList = tableDef.getSecondaryKeyMetaList();
    this.forceIndexName = forceIndexName;
  }

  /**
   * Produces the push down condition for the given
   * relational expression condition.
   *
   * @param condition condition to translate
   * @return push down condition
   */
  public IndexCondition translateMatch(RexNode condition) {
    // does not support disjunctions
    List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
    if (disjunctions.size() == 1) {
      return translateAnd(disjunctions.get(0));
    } else {
      throw new AssertionError("cannot translate " + condition);
    }
  }

  /**
   * Translates a conjunctive predicate to a push down condition.
   *
   * @param condition a conjunctive predicate
   * @return push down condition
   */
  private IndexCondition translateAnd(RexNode condition) {
    // expand calls to SEARCH(..., Sarg()) to >, =, etc.
    final RexNode condition2 =
        RexUtil.expandSearch(rexBuilder, null, condition);
    // decompose condition by AND, flatten row expression
    List<RexNode> rexNodeList = RelOptUtil.conjunctions(condition2);

    List<IndexCondition> indexConditions = new ArrayList<>();

    // try to push down filter by primary key
    if (pkMeta != null) {
      IndexCondition pkPushDownCond = findPushDownCondition(rexNodeList, pkMeta);
      indexConditions.add(pkPushDownCond);
    }

    // try to push down filter by secondary keys
    if (!skMetaList.isEmpty()) {
      for (KeyMeta skMeta : skMetaList) {
        indexConditions.add(findPushDownCondition(rexNodeList, skMeta));
      }
    }

    // a collection of all possible push down conditions, see if it can
    // be pushed down, filter by forcing index name, then sort by comparator
    Stream<IndexCondition> pushDownConditions = indexConditions.stream()
        .filter(IndexCondition::canPushDown)
        .filter(this::nonForceIndexOrMatchForceIndexName)
        .sorted(new IndexConditionComparator());

    return pushDownConditions.findFirst().orElse(IndexCondition.EMPTY_CONDITION);
  }

  /**
   * Tries to translate a conjunctive predicate to push down condition.
   *
   * @param rexNodeList original field expressions
   * @param keyMeta     index metadata
   * @return push down condition
   */
  private IndexCondition findPushDownCondition(List<RexNode> rexNodeList, KeyMeta keyMeta) {
    // find field expressions matching index columns and specific operators
    List<InternalRexNode> matchedRexNodeList = analyzePrefixMatches(rexNodeList, keyMeta);

    // none of the conditions can be pushed down
    if (matchedRexNodeList.isEmpty()) {
      return IndexCondition.EMPTY_CONDITION;
    }

    // a collection that maps ordinal in index column list
    // to multiple field expressions
    Multimap<Integer, InternalRexNode> keyOrdToNodesMap = HashMultimap.create();
    for (InternalRexNode node : matchedRexNodeList) {
      keyOrdToNodesMap.put(node.ordinalInKey, node);
    }

    // left-prefix index rule not match
    Collection<InternalRexNode> leftMostKeyNodes = keyOrdToNodesMap.get(0);
    if (leftMostKeyNodes == null || leftMostKeyNodes.isEmpty()) {
      return IndexCondition.EMPTY_CONDITION;
    }

    // create result which might have conditions to push down
    List<String> indexColumnNames = keyMeta.getKeyColumnNames();
    List<RexNode> pushDownRexNodeList = new ArrayList<>();
    List<RexNode> remainderRexNodeList = new ArrayList<>(rexNodeList);
    IndexCondition condition =
        IndexCondition.create(fieldNames, keyMeta.getName(), indexColumnNames,
            pushDownRexNodeList, remainderRexNodeList);

    // handle point query if possible
    condition =
        handlePointQuery(condition, keyMeta, leftMostKeyNodes,
            keyOrdToNodesMap, pushDownRexNodeList, remainderRexNodeList);
    if (condition.canPushDown()) {
      return condition;
    }

    // handle range query
    condition =
        handleRangeQuery(condition, keyMeta, leftMostKeyNodes,
            pushDownRexNodeList, remainderRexNodeList, ">=", ">");
    condition =
        handleRangeQuery(condition, keyMeta, leftMostKeyNodes,
            pushDownRexNodeList, remainderRexNodeList, "<=", "<");

    return condition;
  }

  /**
   * Analyzes from the first to the subsequent field expression following the
   * left-prefix rule, this will based on a specific index
   * (<code>KeyMeta</code>), check the column and its corresponding operation,
   * see if it can be translated into a push down condition.
   *
   * <p>The result is a collection of matched field expressions.
   *
   * @param rexNodeList Field expressions
   * @param keyMeta     Index metadata
   * @return a collection of matched field expressions
   */
  private List<InternalRexNode> analyzePrefixMatches(List<RexNode> rexNodeList, KeyMeta keyMeta) {
    return rexNodeList.stream()
        .map(rexNode -> translateMatch2(rexNode, keyMeta))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  /**
   * Handles point query push down. The operation of the leftmost nodes
   * should be "=", then we try to find as many "=" operations as
   * possible, if "=" operation found on all index columns, then it is a
   * point query on key (both primary key or composite key), else it will
   * transform to a range query.
   *
   * <p>If conditions can be pushed down, for range query, we only remove
   * first node from field expression list (<code>rexNodeList</code>),
   * because Innodb-java-reader only support range query, not fully
   * index condition pushdown; for point query, we can remove them all.
   */
  private static IndexCondition handlePointQuery(IndexCondition condition,
      KeyMeta keyMeta, Collection<InternalRexNode> leftMostKeyNodes,
      Multimap<Integer, InternalRexNode> keyOrdToNodesMap,
      List<RexNode> pushDownRexNodeList,
      List<RexNode> remainderRexNodeList) {
    Optional<InternalRexNode> leftMostEqOpNode = findFirstOp(leftMostKeyNodes, "=");
    if (leftMostEqOpNode.isPresent()) {
      InternalRexNode node = leftMostEqOpNode.get();

      List<InternalRexNode> matchNodes = Lists.newArrayList(node);
      findSubsequentMatches(matchNodes, keyMeta.getNumOfColumns(), keyOrdToNodesMap, "=");
      List<Object> key = createKey(matchNodes);
      pushDownRexNodeList.add(node.node);
      remainderRexNodeList.remove(node.node);

      if (matchNodes.size() != keyMeta.getNumOfColumns()) {
        // "=" operation does not apply on all index columns
        return condition
            .withQueryType(QueryType.getRangeQuery(keyMeta.isSecondaryKey()))
            .withRangeQueryLowerOp(ComparisonOperator.GTE)
            .withRangeQueryLowerKey(key)
            .withRangeQueryUpperOp(ComparisonOperator.LTE)
            .withRangeQueryUpperKey(key)
            .withPushDownConditions(pushDownRexNodeList)
            .withRemainderConditions(remainderRexNodeList);
      } else {
        for (InternalRexNode n : matchNodes) {
          pushDownRexNodeList.add(n.node);
          remainderRexNodeList.remove(n.node);
        }
        return condition
            .withQueryType(QueryType.getPointQuery(keyMeta.isSecondaryKey()))
            .withPointQueryKey(key)
            .withPushDownConditions(pushDownRexNodeList)
            .withRemainderConditions(remainderRexNodeList);
      }
    }
    return condition;
  }

  /**
   * Handles range query push down. We try to find operation of GTE, GT, LT
   * or LTE in the left most key.
   *
   * <p>We only push down partial condition since Innodb-java-reader only
   * supports range query with lower and upper bound, not fully index condition
   * push down.
   *
   * <p>For example, given the following 7 rows with (a,b) as secondary key.
   * <pre>
   *   a=100,b=200
   *   a=100,b=300
   *   a=100,b=500
   *   a=200,b=100
   *   a=200,b=400
   *   a=300,b=300
   *   a=500,b=600
   * </pre>
   *
   * <p>If condition is <code>a&gt;200 AND b&gt;300</code>,
   * the lower bound should be
   * <code>a=300,b=300</code>, we can only push down one condition
   * <code>a&gt;200</code> as lower bound condition, we cannot push
   * <code>a&gt;200 AND b&gt;300</code> because it will include
   * <code>a=200,b=400</code> as well which is incorrect.
   *
   * <p>If conditions can be pushed down, we will first node from field
   * expression list (<code>rexNodeList</code>).
   */
  private static IndexCondition handleRangeQuery(IndexCondition condition,
      KeyMeta keyMeta, Collection<InternalRexNode> leftMostKeyNodes,
      List<RexNode> pushDownRexNodeList,
      List<RexNode> remainderRexNodeList,
      String... opList) {
    Optional<InternalRexNode> node = findFirstOp(leftMostKeyNodes, opList);
    if (node.isPresent()) {
      pushDownRexNodeList.add(node.get().node);
      remainderRexNodeList.remove(node.get().node);
      List<Object> key = createKey(Lists.newArrayList(node.get()));
      ComparisonOperator op = ComparisonOperator.parse(node.get().op);
      if (ComparisonOperator.isLowerBoundOp(opList)) {
        return condition
            .withQueryType(QueryType.getRangeQuery(keyMeta.isSecondaryKey()))
            .withRangeQueryLowerOp(op)
            .withRangeQueryLowerKey(key)
            .withPushDownConditions(pushDownRexNodeList)
            .withRemainderConditions(remainderRexNodeList);
      } else if (ComparisonOperator.isUpperBoundOp(opList)) {
        return condition
            .withQueryType(QueryType.getRangeQuery(keyMeta.isSecondaryKey()))
            .withRangeQueryUpperOp(op)
            .withRangeQueryUpperKey(key)
            .withPushDownConditions(pushDownRexNodeList)
            .withRemainderConditions(remainderRexNodeList);
      } else {
        throw new AssertionError("comparison operation is invalid " + op);
      }
    }
    return condition;
  }

  /**
   * Translates a binary relation.
   */
  private Optional<InternalRexNode> translateMatch2(RexNode node, KeyMeta keyMeta) {
    switch (node.getKind()) {
    case EQUALS:
      return translateBinary("=", "=", (RexCall) node, keyMeta);
    case LESS_THAN:
      return translateBinary("<", ">", (RexCall) node, keyMeta);
    case LESS_THAN_OR_EQUAL:
      return translateBinary("<=", ">=", (RexCall) node, keyMeta);
    case GREATER_THAN:
      return translateBinary(">", "<", (RexCall) node, keyMeta);
    case GREATER_THAN_OR_EQUAL:
      return translateBinary(">=", "<=", (RexCall) node, keyMeta);
    default:
      return Optional.empty();
    }
  }

  /**
   * Translates a call to a binary operator, reversing arguments if
   * necessary.
   */
  private Optional<InternalRexNode> translateBinary(String op, String rop,
      RexCall call, KeyMeta keyMeta) {
    final RexNode left = call.operands.get(0);
    final RexNode right = call.operands.get(1);
    Optional<InternalRexNode> expression =
        translateBinary2(op, left, right, call, keyMeta);
    if (expression.isPresent()) {
      return expression;
    }
    expression = translateBinary2(rop, right, left, call, keyMeta);
    return expression;
  }

  /**
   * Translates a call to a binary operator. Returns null on failure.
   */
  private Optional<InternalRexNode> translateBinary2(String op, RexNode left,
      RexNode right, RexNode originNode, KeyMeta keyMeta) {
    RexLiteral rightLiteral;
    if (right.isA(SqlKind.LITERAL)) {
      rightLiteral = (RexLiteral) right;
    } else {
      // because MySQL's TIMESTAMP is mapped to TIMESTAMP_WITH_TIME_ZONE sql type,
      // we should cast the value to literal.
      if (right.isA(SqlKind.CAST)
          && isSqlTypeMatch((RexCall) right, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
        rightLiteral = (RexLiteral) ((RexCall) right).operands.get(0);
      } else {
        return Optional.empty();
      }
    }
    switch (left.getKind()) {
    case INPUT_REF:
      final RexInputRef left1 = (RexInputRef) left;
      String name = fieldNames.get(left1.getIndex());
      // filter out field does not show in index column
      if (!keyMeta.getKeyColumnNames().contains(name)) {
        return Optional.empty();
      }
      return translateOp2(op, name, rightLiteral, originNode, keyMeta);
    case CAST:
      return translateBinary2(op, ((RexCall) left).operands.get(0), right,
          originNode, keyMeta);
    default:
      return Optional.empty();
    }
  }

  /**
   * Combines a field name, operator, and literal to produce a predicate string.
   */
  private static Optional<InternalRexNode> translateOp2(String op, String name,
      RexLiteral right, RexNode originNode, KeyMeta keyMeta) {
    String value = literalValue(right);
    InternalRexNode node = new InternalRexNode();
    node.node = originNode;
    node.ordinalInKey = keyMeta.getKeyColumnNames().indexOf(name);
    // For variable length column, Innodb-java-reader have a limitation,
    // left-prefix index length should be less than search value literal.
    // For example, we cannot leverage index of EMAIL(3) upon search value
    // `someone@apache.org`, because the value length is longer than 3.
    if (keyMeta.getVarLen(name).isPresent()
        && keyMeta.getVarLen(name).get() < value.length()) {
      return Optional.empty();
    }
    node.fieldName = name;
    node.op = op;
    node.right = value;
    return Optional.of(node);
  }

  /**
   * Converts the value of a literal to a string.
   *
   * @param literal Literal to translate
   * @return String representation of the literal
   */
  private static String literalValue(RexLiteral literal) {
    switch (literal.getTypeName()) {
    case DATE:
      return String.valueOf(literal.getValueAs(DateString.class));
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return String.valueOf(literal.getValueAs(TimestampString.class));
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
      return String.valueOf(literal.getValueAs(TimeString.class));
    case DECIMAL:
      return String.valueOf(literal.getValue());
    default:
      return String.valueOf(literal.getValue2());
    }
  }

  private static void findSubsequentMatches(List<InternalRexNode> nodes, int numOfKeyColumns,
          Multimap<Integer, InternalRexNode> keyOrdToNodesMap, String op) {
    for (int i = nodes.size(); i < numOfKeyColumns; i++) {
      Optional<InternalRexNode> eqOpNode = findFirstOp(keyOrdToNodesMap.get(i), op);
      if (eqOpNode.isPresent()) {
        nodes.add(eqOpNode.get());
      } else {
        break;
      }
    }
  }

  private static List<Object> createKey(List<InternalRexNode> nodes) {
    return nodes.stream().map(n -> n.right).collect(Collectors.toList());
  }

  /**
   * Finds first node from field expression nodes which match specific
   * operations.
   *
   * <p>If not found, result is {@link Optional#empty()}.
   */
  private static Optional<InternalRexNode> findFirstOp(Collection<InternalRexNode> nodes,
      String... opList) {
    if (nodes.isEmpty()) {
      return Optional.empty();
    }
    for (InternalRexNode node : nodes) {
      for (String op : opList) {
        if (op.equals(node.op)) {
          return Optional.of(node);
        }
      }
    }
    return Optional.empty();
  }

  private boolean nonForceIndexOrMatchForceIndexName(IndexCondition indexCondition) {
    return Optional.ofNullable(forceIndexName)
        .map(indexCondition::nameMatch).orElse(true);
  }

  /** Internal representation of a row expression. */
  private static class InternalRexNode {
    /** Relation expression node. */
    RexNode node;
    /** Field ordinal in indexes. */
    int ordinalInKey;
    /** Field name. */
    String fieldName;
    /** Binary operation like =, >=, <=, > or <.*/
    String op;
    /** Binary operation right literal value. */
    Object right;
  }

  /** Index condition comparator. */
  static class IndexConditionComparator implements Comparator<IndexCondition> {

    @Override public int compare(IndexCondition o1, IndexCondition o2) {
      return Integer.compare(o1.getQueryType().priority(), o2.getQueryType().priority());
    }
  }

  private static boolean isSqlTypeMatch(RexCall rexCall,
      SqlTypeName sqlTypeName) {
    return requireNonNull(rexCall, "rexCall").type.getSqlTypeName()
        == requireNonNull(sqlTypeName, "sqlTypeName");
  }
}
