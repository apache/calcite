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
package org.apache.calcite.tools;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.RepeatUnion;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.TableSpool;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.TransientTable;
import org.apache.calcite.schema.impl.ListTransientTable;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.TableFunctionReturnTypeInference;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Optionality;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Builder for relational expressions.
 *
 * <p>{@code RelBuilder} does not make possible anything that you could not
 * also accomplish by calling the factory methods of the particular relational
 * expression. But it makes common tasks more straightforward and concise.
 *
 * <p>{@code RelBuilder} uses factories to create relational expressions.
 * By default, it uses the default factories, which create logical relational
 * expressions ({@link LogicalFilter},
 * {@link LogicalProject} and so forth).
 * But you could override those factories so that, say, {@code filter} creates
 * instead a {@code HiveFilter}.
 *
 * <p>It is not thread-safe.
 */
public class RelBuilder {
  protected final RelOptCluster cluster;
  protected final RelOptSchema relOptSchema;
  private final RelFactories.FilterFactory filterFactory;
  private final RelFactories.ProjectFactory projectFactory;
  private final RelFactories.AggregateFactory aggregateFactory;
  private final RelFactories.SortFactory sortFactory;
  private final RelFactories.ExchangeFactory exchangeFactory;
  private final RelFactories.SortExchangeFactory sortExchangeFactory;
  private final RelFactories.SetOpFactory setOpFactory;
  private final RelFactories.JoinFactory joinFactory;
  private final RelFactories.CorrelateFactory correlateFactory;
  private final RelFactories.ValuesFactory valuesFactory;
  private final RelFactories.TableScanFactory scanFactory;
  private final RelFactories.TableFunctionScanFactory tableFunctionScanFactory;
  private final RelFactories.SnapshotFactory snapshotFactory;
  private final RelFactories.MatchFactory matchFactory;
  private final RelFactories.SpoolFactory spoolFactory;
  private final RelFactories.RepeatUnionFactory repeatUnionFactory;
  private final Deque<Frame> stack = new ArrayDeque<>();
  private final RexSimplify simplifier;
  private final Config config;

  protected RelBuilder(Context context, RelOptCluster cluster,
      RelOptSchema relOptSchema) {
    this.cluster = cluster;
    this.relOptSchema = relOptSchema;
    if (context == null) {
      context = Contexts.EMPTY_CONTEXT;
    }
    this.config = getConfig(context);
    this.aggregateFactory =
        Util.first(context.unwrap(RelFactories.AggregateFactory.class),
            RelFactories.DEFAULT_AGGREGATE_FACTORY);
    this.filterFactory =
        Util.first(context.unwrap(RelFactories.FilterFactory.class),
            RelFactories.DEFAULT_FILTER_FACTORY);
    this.projectFactory =
        Util.first(context.unwrap(RelFactories.ProjectFactory.class),
            RelFactories.DEFAULT_PROJECT_FACTORY);
    this.sortFactory =
        Util.first(context.unwrap(RelFactories.SortFactory.class),
            RelFactories.DEFAULT_SORT_FACTORY);
    this.exchangeFactory =
        Util.first(context.unwrap(RelFactories.ExchangeFactory.class),
            RelFactories.DEFAULT_EXCHANGE_FACTORY);
    this.sortExchangeFactory =
        Util.first(context.unwrap(RelFactories.SortExchangeFactory.class),
            RelFactories.DEFAULT_SORT_EXCHANGE_FACTORY);
    this.setOpFactory =
        Util.first(context.unwrap(RelFactories.SetOpFactory.class),
            RelFactories.DEFAULT_SET_OP_FACTORY);
    this.joinFactory =
        Util.first(context.unwrap(RelFactories.JoinFactory.class),
            RelFactories.DEFAULT_JOIN_FACTORY);
    this.correlateFactory =
        Util.first(context.unwrap(RelFactories.CorrelateFactory.class),
            RelFactories.DEFAULT_CORRELATE_FACTORY);
    this.valuesFactory =
        Util.first(context.unwrap(RelFactories.ValuesFactory.class),
            RelFactories.DEFAULT_VALUES_FACTORY);
    this.scanFactory =
        Util.first(context.unwrap(RelFactories.TableScanFactory.class),
            RelFactories.DEFAULT_TABLE_SCAN_FACTORY);
    this.tableFunctionScanFactory =
        Util.first(context.unwrap(RelFactories.TableFunctionScanFactory.class),
            RelFactories.DEFAULT_TABLE_FUNCTION_SCAN_FACTORY);
    this.snapshotFactory =
        Util.first(context.unwrap(RelFactories.SnapshotFactory.class),
            RelFactories.DEFAULT_SNAPSHOT_FACTORY);
    this.matchFactory =
        Util.first(context.unwrap(RelFactories.MatchFactory.class),
            RelFactories.DEFAULT_MATCH_FACTORY);
    this.spoolFactory =
        Util.first(context.unwrap(RelFactories.SpoolFactory.class),
            RelFactories.DEFAULT_SPOOL_FACTORY);
    this.repeatUnionFactory =
        Util.first(context.unwrap(RelFactories.RepeatUnionFactory.class),
            RelFactories.DEFAULT_REPEAT_UNION_FACTORY);
    final RexExecutor executor =
        Util.first(context.unwrap(RexExecutor.class),
            Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR));
    final RelOptPredicateList predicates = RelOptPredicateList.EMPTY;
    this.simplifier =
        new RexSimplify(cluster.getRexBuilder(), predicates, executor);
  }

  /** Derives the Config to be used for this RelBuilder.
   *
   * <p>Overrides {@link RelBuilder.Config#simplify} if
   * {@link Hook#REL_BUILDER_SIMPLIFY} is set.
   */
  private Config getConfig(Context context) {
    final Config config =
        Util.first(context.unwrap(Config.class), Config.DEFAULT);
    boolean simplify = Hook.REL_BUILDER_SIMPLIFY.get(config.simplify);
    if (simplify == config.simplify) {
      return config;
    }
    return config.toBuilder().withSimplify(simplify).build();
  }

  /** Creates a RelBuilder. */
  public static RelBuilder create(FrameworkConfig config) {
    return Frameworks.withPrepare(config,
        (cluster, relOptSchema, rootSchema, statement) ->
            new RelBuilder(config.getContext(), cluster, relOptSchema));
  }

  /** Converts this RelBuilder to a string.
   * The string is the string representation of all of the RelNodes on the stack. */
  @Override public String toString() {
    return stack.stream()
        .map(frame -> RelOptUtil.toString(frame.rel))
        .collect(Collectors.joining(""));
  }

  /** Returns the type factory. */
  public RelDataTypeFactory getTypeFactory() {
    return cluster.getTypeFactory();
  }

  /** Returns the builder for {@link RexNode} expressions. */
  public RexBuilder getRexBuilder() {
    return cluster.getRexBuilder();
  }

  /** Creates a {@link RelBuilderFactory}, a partially-created RelBuilder.
   * Just add a {@link RelOptCluster} and a {@link RelOptSchema} */
  public static RelBuilderFactory proto(final Context context) {
    return (cluster, schema) -> new RelBuilder(context, cluster, schema);
  }

  /** Creates a {@link RelBuilderFactory} that uses a given set of factories. */
  public static RelBuilderFactory proto(Object... factories) {
    return proto(Contexts.of(factories));
  }

  public RelOptCluster getCluster() {
    return cluster;
  }

  public RelOptSchema getRelOptSchema() {
    return relOptSchema;
  }

  public RelFactories.TableScanFactory getScanFactory() {
    return scanFactory;
  }

  // Methods for manipulating the stack

  /** Adds a relational expression to be the input to the next relational
   * expression constructed.
   *
   * <p>This method is usual when you want to weave in relational expressions
   * that are not supported by the builder. If, while creating such expressions,
   * you need to use previously built expressions as inputs, call
   * {@link #build()} to pop those inputs. */
  public RelBuilder push(RelNode node) {
    stack.push(new Frame(node));
    return this;
  }

  /** Adds a rel node to the top of the stack while preserving the field names
   * and aliases. */
  private void replaceTop(RelNode node) {
    final Frame frame = stack.pop();
    stack.push(new Frame(node, frame.fields));
  }

  /** Pushes a collection of relational expressions. */
  public RelBuilder pushAll(Iterable<? extends RelNode> nodes) {
    for (RelNode node : nodes) {
      push(node);
    }
    return this;
  }

  /** Returns the final relational expression.
   *
   * <p>Throws if the stack is empty.
   */
  public RelNode build() {
    return stack.pop().rel;
  }

  /** Returns the relational expression at the top of the stack, but does not
   * remove it. */
  public RelNode peek() {
    return peek_().rel;
  }

  private Frame peek_() {
    return stack.peek();
  }

  /** Returns the relational expression {@code n} positions from the top of the
   * stack, but does not remove it. */
  public RelNode peek(int n) {
    return peek_(n).rel;
  }

  private Frame peek_(int n) {
    return Iterables.get(stack, n);
  }

  /** Returns the relational expression {@code n} positions from the top of the
   * stack, but does not remove it. */
  public RelNode peek(int inputCount, int inputOrdinal) {
    return peek_(inputCount, inputOrdinal).rel;
  }

  private Frame peek_(int inputCount, int inputOrdinal) {
    return peek_(inputCount - 1 - inputOrdinal);
  }

  /** Returns the number of fields in all inputs before (to the left of)
   * the given input.
   *
   * @param inputCount Number of inputs
   * @param inputOrdinal Input ordinal
   */
  private int inputOffset(int inputCount, int inputOrdinal) {
    int offset = 0;
    for (int i = 0; i < inputOrdinal; i++) {
      offset += peek(inputCount, i).getRowType().getFieldCount();
    }
    return offset;
  }

  // Methods that return scalar expressions

  /** Creates a literal (constant expression). */
  public RexNode literal(Object value) {
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    if (value == null) {
      final RelDataType type = getTypeFactory().createSqlType(SqlTypeName.NULL);
      return rexBuilder.makeNullLiteral(type);
    } else if (value instanceof Boolean) {
      return rexBuilder.makeLiteral((Boolean) value);
    } else if (value instanceof BigDecimal) {
      return rexBuilder.makeExactLiteral((BigDecimal) value);
    } else if (value instanceof Float || value instanceof Double) {
      return rexBuilder.makeApproxLiteral(
          BigDecimal.valueOf(((Number) value).doubleValue()));
    } else if (value instanceof Number) {
      return rexBuilder.makeExactLiteral(
          BigDecimal.valueOf(((Number) value).longValue()));
    } else if (value instanceof String) {
      return rexBuilder.makeLiteral((String) value);
    } else if (value instanceof Enum) {
      return rexBuilder.makeLiteral(value,
          getTypeFactory().createSqlType(SqlTypeName.SYMBOL), false);
    } else {
      throw new IllegalArgumentException("cannot convert " + value
          + " (" + value.getClass() + ") to a constant");
    }
  }

  /** Creates a correlation variable for the current input, and writes it into
   * a Holder. */
  public RelBuilder variable(Holder<RexCorrelVariable> v) {
    v.set((RexCorrelVariable)
        getRexBuilder().makeCorrel(peek().getRowType(),
            cluster.createCorrel()));
    return this;
  }

  /** Creates a reference to a field by name.
   *
   * <p>Equivalent to {@code field(1, 0, fieldName)}.
   *
   * @param fieldName Field name
   */
  public RexInputRef field(String fieldName) {
    return field(1, 0, fieldName);
  }

  /** Creates a reference to a field of given input relational expression
   * by name.
   *
   * @param inputCount Number of inputs
   * @param inputOrdinal Input ordinal
   * @param fieldName Field name
   */
  public RexInputRef field(int inputCount, int inputOrdinal, String fieldName) {
    final Frame frame = peek_(inputCount, inputOrdinal);
    final List<String> fieldNames = Pair.left(frame.fields());
    int i = fieldNames.indexOf(fieldName);
    if (i >= 0) {
      return field(inputCount, inputOrdinal, i);
    } else {
      throw new IllegalArgumentException("field [" + fieldName
          + "] not found; input fields are: " + fieldNames);
    }
  }

  /** Creates a reference to an input field by ordinal.
   *
   * <p>Equivalent to {@code field(1, 0, ordinal)}.
   *
   * @param fieldOrdinal Field ordinal
   */
  public RexInputRef field(int fieldOrdinal) {
    return (RexInputRef) field(1, 0, fieldOrdinal, false);
  }

  /** Creates a reference to a field of a given input relational expression
   * by ordinal.
   *
   * @param inputCount Number of inputs
   * @param inputOrdinal Input ordinal
   * @param fieldOrdinal Field ordinal within input
   */
  public RexInputRef field(int inputCount, int inputOrdinal, int fieldOrdinal) {
    return (RexInputRef) field(inputCount, inputOrdinal, fieldOrdinal, false);
  }

  /** As {@link #field(int, int, int)}, but if {@code alias} is true, the method
   * may apply an alias to make sure that the field has the same name as in the
   * input frame. If no alias is applied the expression is definitely a
   * {@link RexInputRef}. */
  private RexNode field(int inputCount, int inputOrdinal, int fieldOrdinal,
      boolean alias) {
    final Frame frame = peek_(inputCount, inputOrdinal);
    final RelNode input = frame.rel;
    final RelDataType rowType = input.getRowType();
    if (fieldOrdinal < 0 || fieldOrdinal > rowType.getFieldCount()) {
      throw new IllegalArgumentException("field ordinal [" + fieldOrdinal
          + "] out of range; input fields are: " + rowType.getFieldNames());
    }
    final RelDataTypeField field = rowType.getFieldList().get(fieldOrdinal);
    final int offset = inputOffset(inputCount, inputOrdinal);
    final RexInputRef ref = cluster.getRexBuilder()
        .makeInputRef(field.getType(), offset + fieldOrdinal);
    final RelDataTypeField aliasField = frame.fields().get(fieldOrdinal);
    if (!alias || field.getName().equals(aliasField.getName())) {
      return ref;
    } else {
      return alias(ref, aliasField.getName());
    }
  }

  /** Creates a reference to a field of the current record which originated
   * in a relation with a given alias. */
  public RexNode field(String alias, String fieldName) {
    return field(1, alias, fieldName);
  }

  /** Creates a reference to a field which originated in a relation with the
   * given alias. Searches for the relation starting at the top of the
   * stack. */
  public RexNode field(int inputCount, String alias, String fieldName) {
    Objects.requireNonNull(alias);
    Objects.requireNonNull(fieldName);
    final List<String> fields = new ArrayList<>();
    for (int inputOrdinal = 0; inputOrdinal < inputCount; ++inputOrdinal) {
      final Frame frame = peek_(inputOrdinal);
      for (Ord<Field> p
          : Ord.zip(frame.fields)) {
        // If alias and field name match, reference that field.
        if (p.e.left.contains(alias)
            && p.e.right.getName().equals(fieldName)) {
          return field(inputCount, inputCount - 1 - inputOrdinal, p.i);
        }
        fields.add(
            String.format(Locale.ROOT, "{aliases=%s,fieldName=%s}", p.e.left,
                p.e.right.getName()));
      }
    }
    throw new IllegalArgumentException("no aliased field found; fields are: "
        + fields);
  }

  /** Returns a reference to a given field of a record-valued expression. */
  public RexNode field(RexNode e, String name) {
    return getRexBuilder().makeFieldAccess(e, name, false);
  }

  /** Returns references to the fields of the top input. */
  public ImmutableList<RexNode> fields() {
    return fields(1, 0);
  }

  /** Returns references to the fields of a given input. */
  public ImmutableList<RexNode> fields(int inputCount, int inputOrdinal) {
    final RelNode input = peek(inputCount, inputOrdinal);
    final RelDataType rowType = input.getRowType();
    final ImmutableList.Builder<RexNode> nodes = ImmutableList.builder();
    for (int fieldOrdinal : Util.range(rowType.getFieldCount())) {
      nodes.add(field(inputCount, inputOrdinal, fieldOrdinal));
    }
    return nodes.build();
  }

  /** Returns references to fields for a given collation. */
  public ImmutableList<RexNode> fields(RelCollation collation) {
    final ImmutableList.Builder<RexNode> nodes = ImmutableList.builder();
    for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
      RexNode node = field(fieldCollation.getFieldIndex());
      switch (fieldCollation.direction) {
      case DESCENDING:
        node = desc(node);
      }
      switch (fieldCollation.nullDirection) {
      case FIRST:
        node = nullsFirst(node);
        break;
      case LAST:
        node = nullsLast(node);
        break;
      }
      nodes.add(node);
    }
    return nodes.build();
  }

  /** Returns references to fields for a given list of input ordinals. */
  public ImmutableList<RexNode> fields(List<? extends Number> ordinals) {
    final ImmutableList.Builder<RexNode> nodes = ImmutableList.builder();
    for (Number ordinal : ordinals) {
      RexNode node = field(1, 0, ordinal.intValue(), false);
      nodes.add(node);
    }
    return nodes.build();
  }

  /** Returns references to fields identified by name. */
  public ImmutableList<RexNode> fields(Iterable<String> fieldNames) {
    final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
    for (String fieldName : fieldNames) {
      builder.add(field(fieldName));
    }
    return builder.build();
  }

  /** Returns references to fields identified by a mapping. */
  public ImmutableList<RexNode> fields(Mappings.TargetMapping mapping) {
    return fields(Mappings.asList(mapping));
  }

  /** Creates an access to a field by name. */
  public RexNode dot(RexNode node, String fieldName) {
    final RexBuilder builder = cluster.getRexBuilder();
    return builder.makeFieldAccess(node, fieldName, true);
  }

  /** Creates an access to a field by ordinal. */
  public RexNode dot(RexNode node, int fieldOrdinal) {
    final RexBuilder builder = cluster.getRexBuilder();
    return builder.makeFieldAccess(node, fieldOrdinal);
  }

  /** Creates a call to a scalar operator. */
  public @Nonnull RexNode call(SqlOperator operator, RexNode... operands) {
    return call(operator, ImmutableList.copyOf(operands));
  }

  /** Creates a call to a scalar operator. */
  private @Nonnull RexNode call(SqlOperator operator, List<RexNode> operandList) {
    final RexBuilder builder = cluster.getRexBuilder();
    final RelDataType type = builder.deriveReturnType(operator, operandList);
    return builder.makeCall(type, operator, operandList);
  }

  /** Creates a call to a scalar operator. */
  public @Nonnull RexNode call(SqlOperator operator,
      Iterable<? extends RexNode> operands) {
    return call(operator, ImmutableList.copyOf(operands));
  }

  /** Creates an AND. */
  public RexNode and(RexNode... operands) {
    return and(ImmutableList.copyOf(operands));
  }

  /** Creates an AND.
   *
   * <p>Simplifies the expression a little:
   * {@code e AND TRUE} becomes {@code e};
   * {@code e AND e2 AND NOT e} becomes {@code e2}. */
  public RexNode and(Iterable<? extends RexNode> operands) {
    return RexUtil.composeConjunction(getRexBuilder(), operands);
  }

  /** Creates an OR. */
  public RexNode or(RexNode... operands) {
    return or(ImmutableList.copyOf(operands));
  }

  /** Creates an OR. */
  public RexNode or(Iterable<? extends RexNode> operands) {
    return RexUtil.composeDisjunction(cluster.getRexBuilder(), operands);
  }

  /** Creates a NOT. */
  public RexNode not(RexNode operand) {
    return call(SqlStdOperatorTable.NOT, operand);
  }

  /** Creates an {@code =}. */
  public RexNode equals(RexNode operand0, RexNode operand1) {
    return call(SqlStdOperatorTable.EQUALS, operand0, operand1);
  }

  /** Creates a {@code <>}. */
  public RexNode notEquals(RexNode operand0, RexNode operand1) {
    return call(SqlStdOperatorTable.NOT_EQUALS, operand0, operand1);
  }

  /** Creates a IS NULL. */
  public RexNode isNull(RexNode operand) {
    return call(SqlStdOperatorTable.IS_NULL, operand);
  }

  /** Creates a IS NOT NULL. */
  public RexNode isNotNull(RexNode operand) {
    return call(SqlStdOperatorTable.IS_NOT_NULL, operand);
  }

  /** Creates an expression that casts an expression to a given type. */
  public RexNode cast(RexNode expr, SqlTypeName typeName) {
    final RelDataType type = cluster.getTypeFactory().createSqlType(typeName);
    return cluster.getRexBuilder().makeCast(type, expr);
  }

  /** Creates an expression that casts an expression to a type with a given name
   * and precision or length. */
  public RexNode cast(RexNode expr, SqlTypeName typeName, int precision) {
    final RelDataType type =
        cluster.getTypeFactory().createSqlType(typeName, precision);
    return cluster.getRexBuilder().makeCast(type, expr);
  }

  /** Creates an expression that casts an expression to a type with a given
   * name, precision and scale. */
  public RexNode cast(RexNode expr, SqlTypeName typeName, int precision,
      int scale) {
    final RelDataType type =
        cluster.getTypeFactory().createSqlType(typeName, precision, scale);
    return cluster.getRexBuilder().makeCast(type, expr);
  }

  /**
   * Returns an expression wrapped in an alias.
   *
   * <p>This method is idempotent: If the expression is already wrapped in the
   * correct alias, does nothing; if wrapped in an incorrect alias, removes
   * the incorrect alias and applies the correct alias.
   *
   * @see #project
   */
  public RexNode alias(RexNode expr, String alias) {
    final RexNode aliasLiteral = literal(alias);
    switch (expr.getKind()) {
    case AS:
      final RexCall call = (RexCall) expr;
      if (call.operands.get(1).equals(aliasLiteral)) {
        // current alias is correct
        return expr;
      }
      expr = call.operands.get(0);
      // strip current (incorrect) alias, and fall through
    default:
      return call(SqlStdOperatorTable.AS, expr, aliasLiteral);
    }
  }

  /** Converts a sort expression to descending. */
  public RexNode desc(RexNode node) {
    return call(SqlStdOperatorTable.DESC, node);
  }

  /** Converts a sort expression to nulls last. */
  public RexNode nullsLast(RexNode node) {
    return call(SqlStdOperatorTable.NULLS_LAST, node);
  }

  /** Converts a sort expression to nulls first. */
  public RexNode nullsFirst(RexNode node) {
    return call(SqlStdOperatorTable.NULLS_FIRST, node);
  }

  // Methods that create group keys and aggregate calls

  /** Creates an empty group key. */
  public GroupKey groupKey() {
    return groupKey(ImmutableList.of());
  }

  /** Creates a group key. */
  public GroupKey groupKey(RexNode... nodes) {
    return groupKey(ImmutableList.copyOf(nodes));
  }

  /** Creates a group key. */
  public GroupKey groupKey(Iterable<? extends RexNode> nodes) {
    return new GroupKeyImpl(ImmutableList.copyOf(nodes), null, null);
  }

  /** Creates a group key with grouping sets. */
  public GroupKey groupKey(Iterable<? extends RexNode> nodes,
      Iterable<? extends Iterable<? extends RexNode>> nodeLists) {
    return groupKey_(nodes, nodeLists);
  }

  /** @deprecated Now that indicator is deprecated, use
   * {@link #groupKey(Iterable, Iterable)}, which has the same behavior as
   * calling this method with {@code indicator = false}. */
  @Deprecated // to be removed before 2.0
  public GroupKey groupKey(Iterable<? extends RexNode> nodes, boolean indicator,
      Iterable<? extends Iterable<? extends RexNode>> nodeLists) {
    Aggregate.checkIndicator(indicator);
    return groupKey_(nodes, nodeLists);
  }

  private GroupKey groupKey_(Iterable<? extends RexNode> nodes,
      Iterable<? extends Iterable<? extends RexNode>> nodeLists) {
    final ImmutableList.Builder<ImmutableList<RexNode>> builder =
        ImmutableList.builder();
    for (Iterable<? extends RexNode> nodeList : nodeLists) {
      builder.add(ImmutableList.copyOf(nodeList));
    }
    return new GroupKeyImpl(ImmutableList.copyOf(nodes), builder.build(), null);
  }

  /** Creates a group key of fields identified by ordinal. */
  public GroupKey groupKey(int... fieldOrdinals) {
    return groupKey(fields(ImmutableIntList.of(fieldOrdinals)));
  }

  /** Creates a group key of fields identified by name. */
  public GroupKey groupKey(String... fieldNames) {
    return groupKey(fields(ImmutableList.copyOf(fieldNames)));
  }

  /** Creates a group key, identified by field positions
   * in the underlying relational expression.
   *
   * <p>This method of creating a group key does not allow you to group on new
   * expressions, only column projections, but is efficient, especially when you
   * are coming from an existing {@link Aggregate}. */
  public GroupKey groupKey(@Nonnull ImmutableBitSet groupSet) {
    return groupKey(groupSet, ImmutableList.of(groupSet));
  }

  /** Creates a group key with grouping sets, both identified by field positions
   * in the underlying relational expression.
   *
   * <p>This method of creating a group key does not allow you to group on new
   * expressions, only column projections, but is efficient, especially when you
   * are coming from an existing {@link Aggregate}. */
  public GroupKey groupKey(ImmutableBitSet groupSet,
      @Nonnull Iterable<? extends ImmutableBitSet> groupSets) {
    return groupKey_(groupSet, ImmutableList.copyOf(groupSets));
  }

  /** As {@link #groupKey(ImmutableBitSet, Iterable)}. */
  // deprecated, to be removed before 2.0
  public GroupKey groupKey(ImmutableBitSet groupSet,
      ImmutableList<ImmutableBitSet> groupSets) {
    return groupKey_(groupSet, groupSets == null
        ? ImmutableList.of(groupSet) : ImmutableList.copyOf(groupSets));
  }

  /** @deprecated Use {@link #groupKey(ImmutableBitSet, Iterable)}. */
  @Deprecated // to be removed before 2.0
  public GroupKey groupKey(ImmutableBitSet groupSet, boolean indicator,
      ImmutableList<ImmutableBitSet> groupSets) {
    Aggregate.checkIndicator(indicator);
    return groupKey_(groupSet, groupSets == null
        ? ImmutableList.of(groupSet) : ImmutableList.copyOf(groupSets));
  }

  private GroupKey groupKey_(ImmutableBitSet groupSet,
      @Nonnull ImmutableList<ImmutableBitSet> groupSets) {
    if (groupSet.length() > peek().getRowType().getFieldCount()) {
      throw new IllegalArgumentException("out of bounds: " + groupSet);
    }
    Objects.requireNonNull(groupSets);
    final ImmutableList<RexNode> nodes =
        fields(ImmutableIntList.of(groupSet.toArray()));
    final List<ImmutableList<RexNode>> nodeLists =
        Util.transform(groupSets,
            bitSet -> fields(ImmutableIntList.of(bitSet.toArray())));
    return groupKey_(nodes, nodeLists);
  }

  @Deprecated // to be removed before 2.0
  public AggCall aggregateCall(SqlAggFunction aggFunction, boolean distinct,
      RexNode filter, String alias, RexNode... operands) {
    return aggregateCall(aggFunction, distinct, false, false, filter,
        ImmutableList.of(), alias, ImmutableList.copyOf(operands));
  }

  @Deprecated // to be removed before 2.0
  public AggCall aggregateCall(SqlAggFunction aggFunction, boolean distinct,
      boolean approximate, RexNode filter, String alias, RexNode... operands) {
    return aggregateCall(aggFunction, distinct, approximate, false, filter,
        ImmutableList.of(), alias, ImmutableList.copyOf(operands));
  }

  @Deprecated // to be removed before 2.0
  public AggCall aggregateCall(SqlAggFunction aggFunction, boolean distinct,
      RexNode filter, String alias, Iterable<? extends RexNode> operands) {
    return aggregateCall(aggFunction, distinct, false, false, filter,
        ImmutableList.of(), alias, ImmutableList.copyOf(operands));
  }

  @Deprecated // to be removed before 2.0
  public AggCall aggregateCall(SqlAggFunction aggFunction, boolean distinct,
      boolean approximate, RexNode filter, String alias,
      Iterable<? extends RexNode> operands) {
    return aggregateCall(aggFunction, distinct, approximate, false, filter,
        ImmutableList.of(), alias, ImmutableList.copyOf(operands));
  }

  /** Creates a call to an aggregate function.
   *
   * <p>To add other operands, apply
   * {@link AggCall#distinct()},
   * {@link AggCall#approximate(boolean)},
   * {@link AggCall#filter(RexNode...)},
   * {@link AggCall#sort},
   * {@link AggCall#as} to the result. */
  public AggCall aggregateCall(SqlAggFunction aggFunction,
      Iterable<? extends RexNode> operands) {
    return aggregateCall(aggFunction, false, false, false, null, ImmutableList.of(),
        null, ImmutableList.copyOf(operands));
  }

  /** Creates a call to an aggregate function.
   *
   * <p>To add other operands, apply
   * {@link AggCall#distinct()},
   * {@link AggCall#approximate(boolean)},
   * {@link AggCall#filter(RexNode...)},
   * {@link AggCall#sort},
   * {@link AggCall#as} to the result. */
  public AggCall aggregateCall(SqlAggFunction aggFunction,
      RexNode... operands) {
    return aggregateCall(aggFunction, false, false, false, null, ImmutableList.of(),
        null, ImmutableList.copyOf(operands));
  }

  /** Creates a call to an aggregate function with all applicable operands. */
  protected AggCall aggregateCall(SqlAggFunction aggFunction, boolean distinct,
      boolean approximate, boolean ignoreNulls, RexNode filter, ImmutableList<RexNode> orderKeys,
      String alias, ImmutableList<RexNode> operands) {
    return new AggCallImpl(aggFunction, distinct, approximate, ignoreNulls,
        filter, alias, operands, orderKeys);
  }

  /** Creates a call to the {@code COUNT} aggregate function. */
  public AggCall count(RexNode... operands) {
    return count(false, null, operands);
  }

  /** Creates a call to the {@code COUNT} aggregate function. */
  public AggCall count(Iterable<? extends RexNode> operands) {
    return count(false, null, operands);
  }

  /** Creates a call to the {@code COUNT} aggregate function,
   * optionally distinct and with an alias. */
  public AggCall count(boolean distinct, String alias, RexNode... operands) {
    return aggregateCall(SqlStdOperatorTable.COUNT, distinct, false, false, null,
        ImmutableList.of(), alias, ImmutableList.copyOf(operands));
  }

  /** Creates a call to the {@code COUNT} aggregate function,
   * optionally distinct and with an alias. */
  public AggCall count(boolean distinct, String alias,
      Iterable<? extends RexNode> operands) {
    return aggregateCall(SqlStdOperatorTable.COUNT, distinct, false, false, null,
        ImmutableList.of(), alias, ImmutableList.copyOf(operands));
  }

  /** Creates a call to the {@code COUNT(*)} aggregate function. */
  public AggCall countStar(String alias) {
    return count(false, alias);
  }

  /** Creates a call to the {@code SUM} aggregate function. */
  public AggCall sum(RexNode operand) {
    return sum(false, null, operand);
  }

  /** Creates a call to the {@code SUM} aggregate function,
   * optionally distinct and with an alias. */
  public AggCall sum(boolean distinct, String alias, RexNode operand) {
    return aggregateCall(SqlStdOperatorTable.SUM, distinct, false, false, null,
        ImmutableList.of(), alias, ImmutableList.of(operand));
  }

  /** Creates a call to the {@code AVG} aggregate function. */
  public AggCall avg(RexNode operand) {
    return avg(false, null, operand);
  }

  /** Creates a call to the {@code AVG} aggregate function,
   * optionally distinct and with an alias. */
  public AggCall avg(boolean distinct, String alias, RexNode operand) {
    return aggregateCall(SqlStdOperatorTable.AVG, distinct, false, false, null,
        ImmutableList.of(), alias, ImmutableList.of(operand));
  }

  /** Creates a call to the {@code MIN} aggregate function. */
  public AggCall min(RexNode operand) {
    return min(null, operand);
  }

  /** Creates a call to the {@code MIN} aggregate function,
   * optionally with an alias. */
  public AggCall min(String alias, RexNode operand) {
    return aggregateCall(SqlStdOperatorTable.MIN, false, false, false, null,
        ImmutableList.of(), alias, ImmutableList.of(operand));
  }

  /** Creates a call to the {@code MAX} aggregate function,
   * optionally with an alias. */
  public AggCall max(RexNode operand) {
    return max(null, operand);
  }

  /** Creates a call to the {@code MAX} aggregate function. */
  public AggCall max(String alias, RexNode operand) {
    return aggregateCall(SqlStdOperatorTable.MAX, false, false, false, null,
        ImmutableList.of(), alias, ImmutableList.of(operand));
  }

  // Methods for patterns

  /**
   * Creates a reference to a given field of the pattern.
   *
   * @param alpha the pattern name
   * @param type Type of field
   * @param i Ordinal of field
   * @return Reference to field of pattern
   */
  public RexNode patternField(String alpha, RelDataType type, int i) {
    return getRexBuilder().makePatternFieldRef(alpha, type, i);
  }

  /** Creates a call that concatenates patterns;
   * for use in {@link #match}. */
  public RexNode patternConcat(Iterable<? extends RexNode> nodes) {
    final ImmutableList<RexNode> list = ImmutableList.copyOf(nodes);
    if (list.size() > 2) {
      // Convert into binary calls
      return patternConcat(patternConcat(Util.skipLast(list)), Util.last(list));
    }
    final RelDataType t = getTypeFactory().createSqlType(SqlTypeName.NULL);
    return getRexBuilder().makeCall(t, SqlStdOperatorTable.PATTERN_CONCAT,
        list);
  }

  /** Creates a call that concatenates patterns;
   * for use in {@link #match}. */
  public RexNode patternConcat(RexNode... nodes) {
    return patternConcat(ImmutableList.copyOf(nodes));
  }

  /** Creates a call that creates alternate patterns;
   * for use in {@link #match}. */
  public RexNode patternAlter(Iterable<? extends RexNode> nodes) {
    final RelDataType t = getTypeFactory().createSqlType(SqlTypeName.NULL);
    return getRexBuilder().makeCall(t, SqlStdOperatorTable.PATTERN_ALTER,
        ImmutableList.copyOf(nodes));
  }

  /** Creates a call that creates alternate patterns;
   * for use in {@link #match}. */
  public RexNode patternAlter(RexNode... nodes) {
    return patternAlter(ImmutableList.copyOf(nodes));
  }

  /** Creates a call that creates quantify patterns;
   * for use in {@link #match}. */
  public RexNode patternQuantify(Iterable<? extends RexNode> nodes) {
    final RelDataType t = getTypeFactory().createSqlType(SqlTypeName.NULL);
    return getRexBuilder().makeCall(t, SqlStdOperatorTable.PATTERN_QUANTIFIER,
        ImmutableList.copyOf(nodes));
  }

  /** Creates a call that creates quantify patterns;
   * for use in {@link #match}. */
  public RexNode patternQuantify(RexNode... nodes) {
    return patternQuantify(ImmutableList.copyOf(nodes));
  }

  /** Creates a call that creates permute patterns;
   * for use in {@link #match}. */
  public RexNode patternPermute(Iterable<? extends RexNode> nodes) {
    final RelDataType t = getTypeFactory().createSqlType(SqlTypeName.NULL);
    return getRexBuilder().makeCall(t, SqlStdOperatorTable.PATTERN_PERMUTE,
        ImmutableList.copyOf(nodes));
  }

  /** Creates a call that creates permute patterns;
   * for use in {@link #match}. */
  public RexNode patternPermute(RexNode... nodes) {
    return patternPermute(ImmutableList.copyOf(nodes));
  }

  /** Creates a call that creates an exclude pattern;
   * for use in {@link #match}. */
  public RexNode patternExclude(RexNode node) {
    final RelDataType t = getTypeFactory().createSqlType(SqlTypeName.NULL);
    return getRexBuilder().makeCall(t, SqlStdOperatorTable.PATTERN_EXCLUDE,
        ImmutableList.of(node));
  }

  // Methods that create relational expressions

  /** Creates a {@link TableScan} of the table
   * with a given name.
   *
   * <p>Throws if the table does not exist.
   *
   * <p>Returns this builder.
   *
   * @param tableNames Name of table (can optionally be qualified)
   */
  public RelBuilder scan(Iterable<String> tableNames) {
    final List<String> names = ImmutableList.copyOf(tableNames);
    final RelOptTable relOptTable = relOptSchema.getTableForMember(names);
    if (relOptTable == null) {
      throw RESOURCE.tableNotFound(String.join(".", names)).ex();
    }
    final RelNode scan = scanFactory.createScan(cluster, relOptTable);
    push(scan);
    rename(relOptTable.getRowType().getFieldNames());

    // When the node is not a TableScan but from expansion,
    // we need to explicitly add the alias.
    if (!(scan instanceof TableScan)) {
      as(Util.last(ImmutableList.copyOf(tableNames)));
    }
    return this;
  }

  /** Creates a {@link TableScan} of the table
   * with a given name.
   *
   * <p>Throws if the table does not exist.
   *
   * <p>Returns this builder.
   *
   * @param tableNames Name of table (can optionally be qualified)
   */
  public RelBuilder scan(String... tableNames) {
    return scan(ImmutableList.copyOf(tableNames));
  }

  /** Creates a {@link Snapshot} of a given snapshot period.
   *
   * <p>Returns this builder.
   *
   * @param period Name of table (can optionally be qualified)
   */
  public RelBuilder snapshot(RexNode period) {
    final Frame frame = stack.pop();
    final RelNode snapshot = snapshotFactory.createSnapshot(frame.rel, period);
    stack.push(new Frame(snapshot, frame.fields));
    return this;
  }


  /**
   * Gets column mappings of the operator.
   *
   * @param op operator instance
   * @return column mappings associated with this function
   */
  private Set<RelColumnMapping> getColumnMappings(SqlOperator op) {
    SqlReturnTypeInference inference = op.getReturnTypeInference();
    if (inference instanceof TableFunctionReturnTypeInference) {
      return ((TableFunctionReturnTypeInference) inference).getColumnMappings();
    } else {
      return null;
    }
  }

  /**
   * Creates a RexCall to the {@code CURSOR} function by ordinal.
   *
   * @param inputCount Number of inputs
   * @param ordinal The reference to the relational input
   * @return RexCall to CURSOR function
   */
  public RexNode cursor(int inputCount, int ordinal) {
    if (inputCount <= ordinal || ordinal < 0) {
      throw new IllegalArgumentException("bad input count or ordinal");
    }
    // Refer to the "ordinal"th input as if it were a field
    // (because that's how things are laid out inside a TableFunctionScan)
    final RelNode input = peek(inputCount, ordinal);
    return call(SqlStdOperatorTable.CURSOR,
        getRexBuilder().makeInputRef(input.getRowType(), ordinal));
  }

  /** Creates a {@link TableFunctionScan}. */
  public RelBuilder functionScan(SqlOperator operator,
      int inputCount, RexNode... operands) {
    return functionScan(operator, inputCount, ImmutableList.copyOf(operands));
  }

  /** Creates a {@link TableFunctionScan}. */
  public RelBuilder functionScan(SqlOperator operator,
      int inputCount, Iterable<? extends RexNode> operands) {
    if (inputCount < 0 || inputCount > stack.size()) {
      throw new IllegalArgumentException("bad input count");
    }

    // Gets inputs.
    final List<RelNode> inputs = new LinkedList<>();
    for (int i = 0; i < inputCount; i++) {
      inputs.add(0, build());
    }

    final RexNode call = call(operator, ImmutableList.copyOf(operands));
    final RelNode functionScan =
        tableFunctionScanFactory.createTableFunctionScan(cluster, inputs,
            call, null, getColumnMappings(operator));
    push(functionScan);
    return this;
  }

  /** Creates a {@link Filter} of an array of
   * predicates.
   *
   * <p>The predicates are combined using AND,
   * and optimized in a similar way to the {@link #and} method.
   * If the result is TRUE no filter is created. */
  public RelBuilder filter(RexNode... predicates) {
    return filter(ImmutableSet.of(), ImmutableList.copyOf(predicates));
  }

  /** Creates a {@link Filter} of a list of
   * predicates.
   *
   * <p>The predicates are combined using AND,
   * and optimized in a similar way to the {@link #and} method.
   * If the result is TRUE no filter is created. */
  public RelBuilder filter(Iterable<? extends RexNode> predicates) {
    return filter(ImmutableSet.of(), predicates);
  }

  /** Creates a {@link Filter} of a list of correlation variables
   * and an array of predicates.
   *
   * <p>The predicates are combined using AND,
   * and optimized in a similar way to the {@link #and} method.
   * If the result is TRUE no filter is created. */
  public RelBuilder filter(Iterable<CorrelationId> variablesSet,
      RexNode... predicates) {
    return filter(variablesSet, ImmutableList.copyOf(predicates));
  }

  /**
   * Creates a {@link Filter} of a list of correlation variables
   * and a list of predicates.
   *
   * <p>The predicates are combined using AND,
   * and optimized in a similar way to the {@link #and} method.
   * If the result is TRUE no filter is created. */
  public RelBuilder filter(Iterable<CorrelationId> variablesSet,
      Iterable<? extends RexNode> predicates) {
    final RexNode simplifiedPredicates =
        simplifier.simplifyFilterPredicates(predicates);
    if (simplifiedPredicates == null) {
      return empty();
    }

    if (!simplifiedPredicates.isAlwaysTrue()) {
      final Frame frame = stack.pop();
      final RelNode filter = filterFactory.createFilter(frame.rel,
          simplifiedPredicates, ImmutableSet.copyOf(variablesSet));
      stack.push(new Frame(filter, frame.fields));
    }
    return this;
  }

  /** Creates a {@link Project} of the given
   * expressions. */
  public RelBuilder project(RexNode... nodes) {
    return project(ImmutableList.copyOf(nodes));
  }

  /** Creates a {@link Project} of the given list
   * of expressions.
   *
   * <p>Infers names as would {@link #project(Iterable, Iterable)} if all
   * suggested names were null.
   *
   * @param nodes Expressions
   */
  public RelBuilder project(Iterable<? extends RexNode> nodes) {
    return project(nodes, ImmutableList.of());
  }

  /** Creates a {@link Project} of the given list
   * of expressions and field names.
   *
   * @param nodes Expressions
   * @param fieldNames field names for expressions
   */
  public RelBuilder project(Iterable<? extends RexNode> nodes,
      Iterable<String> fieldNames) {
    return project(nodes, fieldNames, false);
  }

  /** Creates a {@link Project} of all original fields, plus the given
   * expressions. */
  public RelBuilder projectPlus(RexNode... nodes) {
    return projectPlus(ImmutableList.copyOf(nodes));
  }

  /** Creates a {@link Project} of all original fields, plus the given list of
   * expressions. */
  public RelBuilder projectPlus(Iterable<RexNode> nodes) {
    final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
    return project(builder.addAll(fields()).addAll(nodes).build());
  }

  /** Creates a {@link Project} of the given list
   * of expressions, using the given names.
   *
   * <p>Names are deduced as follows:
   * <ul>
   *   <li>If the length of {@code fieldNames} is greater than the index of
   *     the current entry in {@code nodes}, and the entry in
   *     {@code fieldNames} is not null, uses it; otherwise
   *   <li>If an expression projects an input field,
   *     or is a cast an input field,
   *     uses the input field name; otherwise
   *   <li>If an expression is a call to
   *     {@link SqlStdOperatorTable#AS}
   *     (see {@link #alias}), removes the call but uses the intended alias.
   * </ul>
   *
   * <p>After the field names have been inferred, makes the
   * field names unique by appending numeric suffixes.
   *
   * @param nodes Expressions
   * @param fieldNames Suggested field names
   * @param force create project even if it is identity
   */
  public RelBuilder project(
      Iterable<? extends RexNode> nodes,
      Iterable<String> fieldNames,
      boolean force) {
    final Frame frame = stack.peek();
    final RelDataType inputRowType = frame.rel.getRowType();
    final List<RexNode> nodeList = Lists.newArrayList(nodes);

    // Perform a quick check for identity. We'll do a deeper check
    // later when we've derived column names.
    if (!force && Iterables.isEmpty(fieldNames)
        && RexUtil.isIdentity(nodeList, inputRowType)) {
      return this;
    }

    final List<String> fieldNameList = Lists.newArrayList(fieldNames);
    while (fieldNameList.size() < nodeList.size()) {
      fieldNameList.add(null);
    }

    if (frame.rel instanceof Project
        && shouldMergeProject()) {
      final Project project = (Project) frame.rel;
      // Populate field names. If the upper expression is an input ref and does
      // not have a recommended name, use the name of the underlying field.
      for (int i = 0; i < fieldNameList.size(); i++) {
        if (fieldNameList.get(i) == null) {
          final RexNode node = nodeList.get(i);
          if (node instanceof RexInputRef) {
            final RexInputRef ref = (RexInputRef) node;
            fieldNameList.set(i,
                project.getRowType().getFieldNames().get(ref.getIndex()));
          }
        }
      }
      final List<RexNode> newNodes =
          RelOptUtil.pushPastProject(nodeList, project);

      // Carefully build a list of fields, so that table aliases from the input
      // can be seen for fields that are based on a RexInputRef.
      final Frame frame1 = stack.pop();
      final List<Field> fields = new ArrayList<>();
      for (RelDataTypeField f
          : project.getInput().getRowType().getFieldList()) {
        fields.add(new Field(ImmutableSet.of(), f));
      }
      for (Pair<RexNode, Field> pair
          : Pair.zip(project.getProjects(), frame1.fields)) {
        switch (pair.left.getKind()) {
        case INPUT_REF:
          final int i = ((RexInputRef) pair.left).getIndex();
          final Field field = fields.get(i);
          final ImmutableSet<String> aliases = pair.right.left;
          fields.set(i, new Field(aliases, field.right));
          break;
        }
      }
      stack.push(new Frame(project.getInput(), ImmutableList.copyOf(fields)));
      return project(newNodes, fieldNameList, force);
    }

    // Simplify expressions.
    if (config.simplify) {
      for (int i = 0; i < nodeList.size(); i++) {
        nodeList.set(i, simplifier.simplifyPreservingType(nodeList.get(i)));
      }
    }

    // Replace null names with generated aliases.
    for (int i = 0; i < fieldNameList.size(); i++) {
      if (fieldNameList.get(i) == null) {
        fieldNameList.set(i, inferAlias(nodeList, nodeList.get(i), i));
      }
    }

    final ImmutableList.Builder<Field> fields = ImmutableList.builder();
    final Set<String> uniqueNameList =
        getTypeFactory().getTypeSystem().isSchemaCaseSensitive()
        ? new HashSet<>()
        : new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    // calculate final names and build field list
    for (int i = 0; i < fieldNameList.size(); ++i) {
      final RexNode node = nodeList.get(i);
      String name = fieldNameList.get(i);
      String originalName = name;
      Field field;
      if (name == null || uniqueNameList.contains(name)) {
        int j = 0;
        if (name == null) {
          j = i;
        }
        do {
          name = SqlValidatorUtil.F_SUGGESTER.apply(originalName, j, j++);
        } while (uniqueNameList.contains(name));
        fieldNameList.set(i, name);
      }
      RelDataTypeField fieldType =
          new RelDataTypeFieldImpl(name, i, node.getType());
      switch (node.getKind()) {
      case INPUT_REF:
        // preserve rel aliases for INPUT_REF fields
        final int index = ((RexInputRef) node).getIndex();
        field = new Field(frame.fields.get(index).left, fieldType);
        break;
      default:
        field = new Field(ImmutableSet.of(), fieldType);
        break;
      }
      uniqueNameList.add(name);
      fields.add(field);
    }
    if (!force && RexUtil.isIdentity(nodeList, inputRowType)) {
      if (fieldNameList.equals(inputRowType.getFieldNames())) {
        // Do not create an identity project if it does not rename any fields
        return this;
      } else {
        // create "virtual" row type for project only rename fields
        stack.pop();
        stack.push(new Frame(frame.rel, fields.build()));
        return this;
      }
    }
    final RelNode project =
        projectFactory.createProject(frame.rel, ImmutableList.copyOf(nodeList),
            fieldNameList);
    stack.pop();
    stack.push(new Frame(project, fields.build()));
    return this;
  }

  /** Whether to attempt to merge consecutive {@link Project} operators.
   *
   * <p>The default implementation returns {@code true};
   * sub-classes may disable merge by overriding to return {@code false}. */
  @Experimental
  protected boolean shouldMergeProject() {
    return true;
  }

  /** Creates a {@link Project} of the given
   * expressions and field names, and optionally optimizing.
   *
   * <p>If {@code fieldNames} is null, or if a particular entry in
   * {@code fieldNames} is null, derives field names from the input
   * expressions.
   *
   * <p>If {@code force} is false,
   * and the input is a {@code Project},
   * and the expressions  make the trivial projection ($0, $1, ...),
   * modifies the input.
   *
   * @param nodes       Expressions
   * @param fieldNames  Suggested field names, or null to generate
   * @param force       Whether to create a renaming Project if the
   *                    projections are trivial
   */
  public RelBuilder projectNamed(Iterable<? extends RexNode> nodes,
      Iterable<String> fieldNames, boolean force) {
    @SuppressWarnings("unchecked") final List<? extends RexNode> nodeList =
        nodes instanceof List ? (List) nodes : ImmutableList.copyOf(nodes);
    final List<String> fieldNameList =
        fieldNames == null ? null
          : fieldNames instanceof List ? (List<String>) fieldNames
          : ImmutableNullableList.copyOf(fieldNames);
    final RelNode input = peek();
    final RelDataType rowType =
        RexUtil.createStructType(cluster.getTypeFactory(), nodeList,
            fieldNameList, SqlValidatorUtil.F_SUGGESTER);
    if (!force
        && RexUtil.isIdentity(nodeList, input.getRowType())) {
      if (input instanceof Project && fieldNames != null) {
        // Rename columns of child projection if desired field names are given.
        final Frame frame = stack.pop();
        final Project childProject = (Project) frame.rel;
        final Project newInput = childProject.copy(childProject.getTraitSet(),
            childProject.getInput(), childProject.getProjects(), rowType);
        stack.push(new Frame(newInput, frame.fields));
      }
    } else {
      project(nodeList, rowType.getFieldNames(), force);
    }
    return this;
  }

  /** Ensures that the field names match those given.
   *
   * <p>If all fields have the same name, adds nothing;
   * if any fields do not have the same name, adds a {@link Project}.
   *
   * <p>Note that the names can be short-lived. Other {@code RelBuilder}
   * operations make no guarantees about the field names of the rows they
   * produce.
   *
   * @param fieldNames List of desired field names; may contain null values or
   * have fewer fields than the current row type
   */
  public RelBuilder rename(List<String> fieldNames) {
    final List<String> oldFieldNames = peek().getRowType().getFieldNames();
    Preconditions.checkArgument(fieldNames.size() <= oldFieldNames.size(),
        "More names than fields");
    final List<String> newFieldNames = new ArrayList<>(oldFieldNames);
    for (int i = 0; i < fieldNames.size(); i++) {
      final String s = fieldNames.get(i);
      if (s != null) {
        newFieldNames.set(i, s);
      }
    }
    if (oldFieldNames.equals(newFieldNames)) {
      return this;
    }
    if (peek() instanceof Values) {
      // Special treatment for VALUES. Re-build it rather than add a project.
      final Values v = (Values) build();
      final RelDataTypeFactory.Builder b = getTypeFactory().builder();
      for (Pair<String, RelDataTypeField> p
          : Pair.zip(newFieldNames, v.getRowType().getFieldList())) {
        b.add(p.left, p.right.getType());
      }
      return values(v.tuples, b.build());
    }

    return project(fields(), newFieldNames, true);
  }

  /** Infers the alias of an expression.
   *
   * <p>If the expression was created by {@link #alias}, replaces the expression
   * in the project list.
   */
  private String inferAlias(List<RexNode> exprList, RexNode expr, int i) {
    switch (expr.getKind()) {
    case INPUT_REF:
      final RexInputRef ref = (RexInputRef) expr;
      return stack.peek().fields.get(ref.getIndex()).getValue().getName();
    case CAST:
      return inferAlias(exprList, ((RexCall) expr).getOperands().get(0), -1);
    case AS:
      final RexCall call = (RexCall) expr;
      if (i >= 0) {
        exprList.set(i, call.getOperands().get(0));
      }
      return ((NlsString) ((RexLiteral) call.getOperands().get(1)).getValue())
          .getValue();
    default:
      return null;
    }
  }

  /** Creates an {@link Aggregate} that makes the
   * relational expression distinct on all fields. */
  public RelBuilder distinct() {
    return aggregate(groupKey(fields()));
  }

  /** Creates an {@link Aggregate} with an array of
   * calls. */
  public RelBuilder aggregate(GroupKey groupKey, AggCall... aggCalls) {
    return aggregate(groupKey, ImmutableList.copyOf(aggCalls));
  }

  /** Creates an {@link Aggregate} with a list of
   * calls. */
  public RelBuilder aggregate(GroupKey groupKey, Iterable<AggCall> aggCalls) {
    final Registrar registrar =
        new Registrar(fields(), peek().getRowType().getFieldNames());
    final GroupKeyImpl groupKey_ = (GroupKeyImpl) groupKey;
    final ImmutableBitSet groupSet =
        ImmutableBitSet.of(registrar.registerExpressions(groupKey_.nodes));
  label:
    if (Iterables.isEmpty(aggCalls)) {
      final RelMetadataQuery mq = peek().getCluster().getMetadataQuery();
      if (groupSet.isEmpty()) {
        final Double minRowCount = mq.getMinRowCount(peek());
        if (minRowCount == null || minRowCount < 1D) {
          // We can't remove "GROUP BY ()" if there's a chance the rel could be
          // empty.
          break label;
        }
      }
      if (registrar.extraNodes.size() == fields().size()) {
        final Boolean unique = mq.areColumnsUnique(peek(), groupSet);
        if (unique != null && unique) {
          // Rel is already unique.
          return project(fields(groupSet.asList()));
        }
      }
      final Double maxRowCount = mq.getMaxRowCount(peek());
      if (maxRowCount != null && maxRowCount <= 1D) {
        // If there is at most one row, rel is already unique.
        return project(fields(groupSet.asList()));
      }
    }
    final ImmutableList<ImmutableBitSet> groupSets;
    if (groupKey_.nodeLists != null) {
      final int sizeBefore = registrar.extraNodes.size();
      final SortedSet<ImmutableBitSet> groupSetSet =
          new TreeSet<>(ImmutableBitSet.ORDERING);
      for (ImmutableList<RexNode> nodeList : groupKey_.nodeLists) {
        final ImmutableBitSet groupSet2 =
            ImmutableBitSet.of(registrar.registerExpressions(nodeList));
        if (!groupSet.contains(groupSet2)) {
          throw new IllegalArgumentException("group set element " + nodeList
              + " must be a subset of group key");
        }
        groupSetSet.add(groupSet2);
      }
      groupSets = ImmutableList.copyOf(groupSetSet);
      if (registrar.extraNodes.size() > sizeBefore) {
        throw new IllegalArgumentException(
            "group sets contained expressions not in group key: "
                + registrar.extraNodes.subList(sizeBefore,
                registrar.extraNodes.size()));
      }
    } else {
      groupSets = ImmutableList.of(groupSet);
    }
    for (AggCall aggCall : aggCalls) {
      if (aggCall instanceof AggCallImpl) {
        final AggCallImpl aggCall1 = (AggCallImpl) aggCall;
        registrar.registerExpressions(aggCall1.operands);
        if (aggCall1.filter != null) {
          registrar.registerExpression(aggCall1.filter);
        }
      }
    }
    project(registrar.extraNodes);
    rename(registrar.names);
    final Frame frame = stack.pop();
    final RelNode r = frame.rel;
    final List<AggregateCall> aggregateCalls = new ArrayList<>();
    for (AggCall aggCall : aggCalls) {
      final AggregateCall aggregateCall;
      if (aggCall instanceof AggCallImpl) {
        final AggCallImpl aggCall1 = (AggCallImpl) aggCall;
        final List<Integer> args =
            registrar.registerExpressions(aggCall1.operands);
        final int filterArg = aggCall1.filter == null ? -1
            : registrar.registerExpression(aggCall1.filter);
        if (aggCall1.distinct && !aggCall1.aggFunction.isQuantifierAllowed()) {
          throw new IllegalArgumentException("DISTINCT not allowed");
        }
        if (aggCall1.filter != null && !aggCall1.aggFunction.allowsFilter()) {
          throw new IllegalArgumentException("FILTER not allowed");
        }
        RelCollation collation =
            RelCollations.of(aggCall1.orderKeys
                .stream()
                .map(orderKey ->
                    collation(orderKey, RelFieldCollation.Direction.ASCENDING,
                        null, Collections.emptyList()))
                .collect(Collectors.toList()));
        aggregateCall =
            AggregateCall.create(aggCall1.aggFunction, aggCall1.distinct,
                aggCall1.approximate,
                aggCall1.ignoreNulls, args, filterArg, collation,
                groupSet.cardinality(), r, null, aggCall1.alias);
      } else {
        aggregateCall = ((AggCallImpl2) aggCall).aggregateCall;
      }
      aggregateCalls.add(aggregateCall);
    }

    assert ImmutableBitSet.ORDERING.isStrictlyOrdered(groupSets) : groupSets;
    for (ImmutableBitSet set : groupSets) {
      assert groupSet.contains(set);
    }

    if (!config.dedupAggregateCalls || Util.isDistinct(aggregateCalls)) {
      return aggregate_(groupSet, groupSets, r, aggregateCalls,
          registrar.extraNodes, frame.fields);
    }

    // There are duplicate aggregate calls. Rebuild the list to eliminate
    // duplicates, then add a Project.
    final Set<AggregateCall> callSet = new HashSet<>();
    final List<Pair<Integer, String>> projects = new ArrayList<>();
    Util.range(groupSet.cardinality())
        .forEach(i -> projects.add(Pair.of(i, null)));
    final List<AggregateCall> distinctAggregateCalls = new ArrayList<>();
    for (AggregateCall aggregateCall : aggregateCalls) {
      final int i;
      if (callSet.add(aggregateCall)) {
        i = distinctAggregateCalls.size();
        distinctAggregateCalls.add(aggregateCall);
      } else {
        i = distinctAggregateCalls.indexOf(aggregateCall);
        assert i >= 0;
      }
      projects.add(Pair.of(groupSet.cardinality() + i, aggregateCall.name));
    }
    aggregate_(groupSet, groupSets, r, distinctAggregateCalls,
        registrar.extraNodes, frame.fields);
    final List<RexNode> fields = projects.stream()
        .map(p -> p.right == null ? field(p.left)
            : alias(field(p.left), p.right))
        .collect(Collectors.toList());
    return project(fields);
  }

  /** Finishes the implementation of {@link #aggregate} by creating an
   * {@link Aggregate} and pushing it onto the stack. */
  private RelBuilder aggregate_(ImmutableBitSet groupSet,
      ImmutableList<ImmutableBitSet> groupSets, RelNode input,
      List<AggregateCall> aggregateCalls, List<RexNode> extraNodes,
      ImmutableList<Field> inFields) {
    final RelNode aggregate = aggregateFactory.createAggregate(input,
        groupSet, groupSets, aggregateCalls);

    // build field list
    final ImmutableList.Builder<Field> fields = ImmutableList.builder();
    final List<RelDataTypeField> aggregateFields =
        aggregate.getRowType().getFieldList();
    int i = 0;
    // first, group fields
    for (Integer groupField : groupSet.asList()) {
      RexNode node = extraNodes.get(groupField);
      final SqlKind kind = node.getKind();
      switch (kind) {
      case INPUT_REF:
        fields.add(inFields.get(((RexInputRef) node).getIndex()));
        break;
      default:
        String name = aggregateFields.get(i).getName();
        RelDataTypeField fieldType =
            new RelDataTypeFieldImpl(name, i, node.getType());
        fields.add(new Field(ImmutableSet.of(), fieldType));
        break;
      }
      i++;
    }
    // second, aggregate fields. retain `i' as field index
    for (int j = 0; j < aggregateCalls.size(); ++j) {
      final AggregateCall call = aggregateCalls.get(j);
      final RelDataTypeField fieldType =
          new RelDataTypeFieldImpl(aggregateFields.get(i + j).getName(), i + j,
              call.getType());
      fields.add(new Field(ImmutableSet.of(), fieldType));
    }
    stack.push(new Frame(aggregate, fields.build()));
    return this;
  }

  private RelBuilder setOp(boolean all, SqlKind kind, int n) {
    List<RelNode> inputs = new LinkedList<>();
    for (int i = 0; i < n; i++) {
      inputs.add(0, build());
    }
    switch (kind) {
    case UNION:
    case INTERSECT:
    case EXCEPT:
      if (n < 1) {
        throw new IllegalArgumentException(
            "bad INTERSECT/UNION/EXCEPT input count");
      }
      break;
    default:
      throw new AssertionError("bad setOp " + kind);
    }
    switch (n) {
    case 1:
      return push(inputs.get(0));
    default:
      return push(setOpFactory.createSetOp(kind, inputs, all));
    }
  }

  /** Creates a {@link Union} of the two most recent
   * relational expressions on the stack.
   *
   * @param all Whether to create UNION ALL
   */
  public RelBuilder union(boolean all) {
    return union(all, 2);
  }

  /** Creates a {@link Union} of the {@code n}
   * most recent relational expressions on the stack.
   *
   * @param all Whether to create UNION ALL
   * @param n Number of inputs to the UNION operator
   */
  public RelBuilder union(boolean all, int n) {
    return setOp(all, SqlKind.UNION, n);
  }

  /** Creates an {@link Intersect} of the two most
   * recent relational expressions on the stack.
   *
   * @param all Whether to create INTERSECT ALL
   */
  public RelBuilder intersect(boolean all) {
    return intersect(all, 2);
  }

  /** Creates an {@link Intersect} of the {@code n}
   * most recent relational expressions on the stack.
   *
   * @param all Whether to create INTERSECT ALL
   * @param n Number of inputs to the INTERSECT operator
   */
  public RelBuilder intersect(boolean all, int n) {
    return setOp(all, SqlKind.INTERSECT, n);
  }

  /** Creates a {@link Minus} of the two most recent
   * relational expressions on the stack.
   *
   * @param all Whether to create EXCEPT ALL
   */
  public RelBuilder minus(boolean all) {
    return minus(all, 2);
  }

  /** Creates a {@link Minus} of the {@code n}
   * most recent relational expressions on the stack.
   *
   * @param all Whether to create EXCEPT ALL
   */
  public RelBuilder minus(boolean all, int n) {
    return setOp(all, SqlKind.EXCEPT, n);
  }

  /**
   * Creates a {@link TableScan} on a {@link TransientTable} with the given name, using as type
   * the top of the stack's type.
   *
   * @param tableName table name
   */
  @Experimental
  public RelBuilder transientScan(String tableName) {
    return this.transientScan(tableName, this.peek().getRowType());
  }

  /**
   * Creates a {@link TableScan} on a {@link TransientTable} with the given name and type.
   *
   * @param tableName table name
   * @param rowType row type of the table
   */
  @Experimental
  public RelBuilder transientScan(String tableName, RelDataType rowType) {
    TransientTable transientTable = new ListTransientTable(tableName, rowType);
    RelOptTable relOptTable = RelOptTableImpl.create(
        relOptSchema,
        rowType,
        transientTable,
        ImmutableList.of(tableName));
    RelNode scan = scanFactory.createScan(cluster, relOptTable);
    push(scan);
    rename(rowType.getFieldNames());
    return this;
  }

  /**
   * Creates a {@link TableSpool} for the most recent relational expression.
   *
   * @param readType Spool's read type (as described in {@link Spool.Type})
   * @param writeType Spool's write type (as described in {@link Spool.Type})
   * @param table Table to write into
   */
  private RelBuilder tableSpool(Spool.Type readType, Spool.Type writeType, RelOptTable table) {
    RelNode spool =  spoolFactory.createTableSpool(peek(), readType, writeType, table);
    replaceTop(spool);
    return this;
  }

  /**
   * Creates a {@link RepeatUnion} associated to a {@link TransientTable} without a maximum number
   * of iterations, i.e. repeatUnion(tableName, all, -1).
   *
   * @param tableName name of the {@link TransientTable} associated to the {@link RepeatUnion}
   * @param all whether duplicates will be considered or not
   */
  @Experimental
  public RelBuilder repeatUnion(String tableName, boolean all) {
    return repeatUnion(tableName, all, -1);
  }

  /**
   * Creates a {@link RepeatUnion} associated to a {@link TransientTable} of the
   * two most recent relational expressions on the stack.
   *
   * <p>Warning: if these relational expressions are not
   * correctly defined, this operation might lead to an infinite loop.
   *
   * <p>The generated {@link RepeatUnion} operates as follows:
   *
   * <ul>
   * <li>Evaluate its left term once, propagating the results into the
   *     {@link TransientTable};
   * <li>Evaluate its right term (which may contain a {@link TableScan} on the
   *     {@link TransientTable}) over and over until it produces no more results
   *     (or until an optional maximum number of iterations is reached). On each
   *     iteration, the results are propagated into the {@link TransientTable},
   *     overwriting the results from the previous one.
   * </ul>
   *
   * @param tableName Name of the {@link TransientTable} associated to the
   *     {@link RepeatUnion}
   * @param all Whether duplicates are considered
   * @param iterationLimit Maximum number of iterations; negative value means no limit
   */
  @Experimental
  public RelBuilder repeatUnion(String tableName, boolean all, int iterationLimit) {
    RelOptTableFinder finder = new RelOptTableFinder(tableName);
    for (int i = 0; i < stack.size(); i++) { // search scan(tableName) in the stack
      peek(i).accept(finder);
      if (finder.relOptTable != null) { // found
        break;
      }
    }
    if (finder.relOptTable == null) {
      throw RESOURCE.tableNotFound(tableName).ex();
    }

    RelNode iterative = tableSpool(Spool.Type.LAZY, Spool.Type.LAZY, finder.relOptTable).build();
    RelNode seed = tableSpool(Spool.Type.LAZY, Spool.Type.LAZY, finder.relOptTable).build();
    RelNode repUnion = repeatUnionFactory.createRepeatUnion(seed, iterative, all, iterationLimit);
    return push(repUnion);
  }

  /**
   * Auxiliary class to find a certain RelOptTable based on its name
   */
  private static final class RelOptTableFinder extends RelHomogeneousShuttle {
    private RelOptTable relOptTable = null;
    private final String tableName;

    private RelOptTableFinder(String tableName) {
      this.tableName = tableName;
    }

    @Override public RelNode visit(TableScan scan) {
      final RelOptTable scanTable = scan.getTable();
      final List<String> qualifiedName = scanTable.getQualifiedName();
      if (qualifiedName.get(qualifiedName.size() - 1).equals(tableName)) {
        relOptTable = scanTable;
      }
      return super.visit(scan);
    }
  }

  /** Creates a {@link Join}. */
  public RelBuilder join(JoinRelType joinType, RexNode condition0,
      RexNode... conditions) {
    return join(joinType, Lists.asList(condition0, conditions));
  }

  /** Creates a {@link Join} with multiple
   * conditions. */
  public RelBuilder join(JoinRelType joinType,
      Iterable<? extends RexNode> conditions) {
    return join(joinType, and(conditions),
        ImmutableSet.of());
  }

  public RelBuilder join(JoinRelType joinType, RexNode condition) {
    return join(joinType, condition, ImmutableSet.of());
  }

  /** Creates a {@link Join} with correlating
   * variables. */
  public RelBuilder join(JoinRelType joinType, RexNode condition,
      Set<CorrelationId> variablesSet) {
    Frame right = stack.pop();
    final Frame left = stack.pop();
    final RelNode join;
    final boolean correlate = variablesSet.size() == 1;
    RexNode postCondition = literal(true);
    if (config.simplify) {
      // Normalize expanded versions IS NOT DISTINCT FROM so that simplifier does not
      // transform the expression to something unrecognizable
      if (condition instanceof RexCall) {
        condition = RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall) condition,
            getRexBuilder());
      }
      condition = simplifier.simplifyUnknownAsFalse(condition);
    }
    if (correlate) {
      final CorrelationId id = Iterables.getOnlyElement(variablesSet);
      final ImmutableBitSet requiredColumns =
          RelOptUtil.correlationColumns(id, right.rel);
      if (!RelOptUtil.notContainsCorrelation(left.rel, id, Litmus.IGNORE)) {
        throw new IllegalArgumentException("variable " + id
            + " must not be used by left input to correlation");
      }
      switch (joinType) {
      case LEFT:
        // Correlate does not have an ON clause.
        // For a LEFT correlate, predicate must be evaluated first.
        // For INNER, we can defer.
        stack.push(right);
        filter(condition.accept(new Shifter(left.rel, id, right.rel)));
        right = stack.pop();
        break;
      default:
        postCondition = condition;
      }
      join = correlateFactory.createCorrelate(left.rel, right.rel, id,
          requiredColumns, joinType);
    } else {
      join = joinFactory.createJoin(left.rel, right.rel, condition,
          variablesSet, joinType, false);
    }
    final ImmutableList.Builder<Field> fields = ImmutableList.builder();
    fields.addAll(left.fields);
    fields.addAll(right.fields);
    stack.push(new Frame(join, fields.build()));
    filter(postCondition);
    return this;
  }

  /** Creates a {@link Correlate}
   * with a {@link CorrelationId} and an array of fields that are used by correlation. */
  public RelBuilder correlate(JoinRelType joinType,
      CorrelationId correlationId, RexNode... requiredFields) {
    return correlate(joinType, correlationId, ImmutableList.copyOf(requiredFields));
  }

  /** Creates a {@link Correlate}
   * with a {@link CorrelationId} and a list of fields that are used by correlation. */
  public RelBuilder correlate(JoinRelType joinType,
      CorrelationId correlationId, Iterable<? extends RexNode> requiredFields) {
    Frame right = stack.pop();

    final Registrar registrar =
        new Registrar(fields(), peek().getRowType().getFieldNames());

    List<Integer> requiredOrdinals =
        registrar.registerExpressions(ImmutableList.copyOf(requiredFields));

    project(registrar.extraNodes);
    rename(registrar.names);
    Frame left = stack.pop();

    final RelNode correlate = correlateFactory
        .createCorrelate(left.rel, right.rel, correlationId,
            ImmutableBitSet.of(requiredOrdinals), joinType);

    final ImmutableList.Builder<Field> fields = ImmutableList.builder();
    fields.addAll(left.fields);
    fields.addAll(right.fields);
    stack.push(new Frame(correlate, fields.build()));

    return this;
  }

  /** Creates a {@link Join} using USING syntax.
   *
   * <p>For each of the field names, both left and right inputs must have a
   * field of that name. Constructs a join condition that the left and right
   * fields are equal.
   *
   * @param joinType Join type
   * @param fieldNames Field names
   */
  public RelBuilder join(JoinRelType joinType, String... fieldNames) {
    final List<RexNode> conditions = new ArrayList<>();
    for (String fieldName : fieldNames) {
      conditions.add(
          call(SqlStdOperatorTable.EQUALS,
              field(2, 0, fieldName),
              field(2, 1, fieldName)));
    }
    return join(joinType, conditions);
  }

  /** Creates a {@link Join} with {@link JoinRelType#SEMI}.
   *
   * <p>A semi-join is a form of join that combines two relational expressions
   * according to some condition, and outputs only rows from the left input for
   * which at least one row from the right input matches. It only outputs
   * columns from the left input, and ignores duplicates on the right.
   *
   * <p>For example, {@code EMP semi-join DEPT} finds all {@code EMP} records
   * that do not have a corresponding {@code DEPT} record, similar to the
   * following SQL:
   *
   * <blockquote><pre>
   * SELECT * FROM EMP
   * WHERE EXISTS (SELECT 1 FROM DEPT
   *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
   * </blockquote>
   */
  public RelBuilder semiJoin(Iterable<? extends RexNode> conditions) {
    final Frame right = stack.pop();
    final RelNode semiJoin =
        joinFactory.createJoin(peek(), right.rel,
            and(conditions), ImmutableSet.of(), JoinRelType.SEMI, false);
    replaceTop(semiJoin);
    return this;
  }

  /** Creates a {@link Join} with {@link JoinRelType#SEMI}.
   *
   * @see #semiJoin(Iterable) */
  public RelBuilder semiJoin(RexNode... conditions) {
    return semiJoin(ImmutableList.copyOf(conditions));
  }

  /** Creates an anti-join.
   *
   * <p>An anti-join is a form of join that combines two relational expressions
   * according to some condition, but outputs only rows from the left input
   * for which no rows from the right input match.
   *
   * <p>For example, {@code EMP anti-join DEPT} finds all {@code EMP} records
   * that do not have a corresponding {@code DEPT} record, similar to the
   * following SQL:
   *
   * <blockquote><pre>
   * SELECT * FROM EMP
   * WHERE NOT EXISTS (SELECT 1 FROM DEPT
   *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
   * </blockquote>
   */
  public RelBuilder antiJoin(Iterable<? extends RexNode> conditions) {
    final Frame right = stack.pop();
    final RelNode antiJoin =
        joinFactory.createJoin(peek(), right.rel,
            and(conditions), ImmutableSet.of(), JoinRelType.ANTI, false);
    replaceTop(antiJoin);
    return this;
  }

  /** Creates an anti-join.
   *
   * @see #antiJoin(Iterable) */
  public RelBuilder antiJoin(RexNode... conditions) {
    return antiJoin(ImmutableList.copyOf(conditions));
  }

  /** Assigns a table alias to the top entry on the stack. */
  public RelBuilder as(final String alias) {
    final Frame pair = stack.pop();
    List<Field> newFields =
        Util.transform(pair.fields, field -> field.addAlias(alias));
    stack.push(new Frame(pair.rel, ImmutableList.copyOf(newFields)));
    return this;
  }

  /** Creates a {@link Values}.
   *
   * <p>The {@code values} array must have the same number of entries as
   * {@code fieldNames}, or an integer multiple if you wish to create multiple
   * rows.
   *
   * <p>If there are zero rows, or if all values of a any column are
   * null, this method cannot deduce the type of columns. For these cases,
   * call {@link #values(Iterable, RelDataType)}.
   *
   * @param fieldNames Field names
   * @param values Values
   */
  public RelBuilder values(String[] fieldNames, Object... values) {
    if (fieldNames == null
        || fieldNames.length == 0
        || values.length % fieldNames.length != 0
        || values.length < fieldNames.length) {
      throw new IllegalArgumentException(
          "Value count must be a positive multiple of field count");
    }
    final int rowCount = values.length / fieldNames.length;
    for (Ord<String> fieldName : Ord.zip(fieldNames)) {
      if (allNull(values, fieldName.i, fieldNames.length)) {
        throw new IllegalArgumentException("All values of field '" + fieldName.e
            + "' are null; cannot deduce type");
      }
    }
    final ImmutableList<ImmutableList<RexLiteral>> tupleList =
        tupleList(fieldNames.length, values);
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (final Ord<String> fieldName : Ord.zip(fieldNames)) {
      final String name =
          fieldName.e != null ? fieldName.e : "expr$" + fieldName.i;
      final RelDataType type = typeFactory.leastRestrictive(
          new AbstractList<RelDataType>() {
            public RelDataType get(int index) {
              return tupleList.get(index).get(fieldName.i).getType();
            }

            public int size() {
              return rowCount;
            }
          });
      builder.add(name, type);
    }
    final RelDataType rowType = builder.build();
    return values(tupleList, rowType);
  }

  private ImmutableList<ImmutableList<RexLiteral>> tupleList(int columnCount,
      Object[] values) {
    final ImmutableList.Builder<ImmutableList<RexLiteral>> listBuilder =
        ImmutableList.builder();
    final List<RexLiteral> valueList = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
      valueList.add((RexLiteral) literal(value));
      if ((i + 1) % columnCount == 0) {
        listBuilder.add(ImmutableList.copyOf(valueList));
        valueList.clear();
      }
    }
    return listBuilder.build();
  }

  /** Returns whether all values for a given column are null. */
  private boolean allNull(Object[] values, int column, int columnCount) {
    for (int i = column; i < values.length; i += columnCount) {
      if (values[i] != null) {
        return false;
      }
    }
    return true;
  }

  /** Creates a relational expression that reads from an input and throws
   * all of the rows away.
   *
   * <p>Note that this method always pops one relational expression from the
   * stack. {@code values}, in contrast, does not pop any relational
   * expressions, and always produces a leaf.
   *
   * <p>The default implementation creates a {@link Values} with the same
   * specified row type and aliases as the input, and ignores the input entirely.
   * But schema-on-query systems such as Drill might override this method to
   * create a relation expression that retains the input, just to read its
   * schema.
   */
  public RelBuilder empty() {
    final Frame frame = stack.pop();
    final RelNode values =
        valuesFactory.createValues(cluster, frame.rel.getRowType(), ImmutableList.of());
    stack.push(new Frame(values, frame.fields));
    return this;
  }

  /** Creates a {@link Values} with a specified row type.
   *
   * <p>This method can handle cases that {@link #values(String[], Object...)}
   * cannot, such as all values of a column being null, or there being zero
   * rows.
   *
   * @param rowType Row type
   * @param columnValues Values
   */
  public RelBuilder values(RelDataType rowType, Object... columnValues) {
    final ImmutableList<ImmutableList<RexLiteral>> tupleList =
        tupleList(rowType.getFieldCount(), columnValues);
    RelNode values = valuesFactory.createValues(cluster, rowType,
        ImmutableList.copyOf(tupleList));
    push(values);
    return this;
  }

  /** Creates a {@link Values} with a specified row type.
   *
   * <p>This method can handle cases that {@link #values(String[], Object...)}
   * cannot, such as all values of a column being null, or there being zero
   * rows.
   *
   * @param tupleList Tuple list
   * @param rowType Row type
   */
  public RelBuilder values(Iterable<? extends List<RexLiteral>> tupleList,
      RelDataType rowType) {
    RelNode values =
        valuesFactory.createValues(cluster, rowType, copy(tupleList));
    push(values);
    return this;
  }

  /** Creates a {@link Values} with a specified row type and
   * zero rows.
   *
   * @param rowType Row type
   */
  public RelBuilder values(RelDataType rowType) {
    return values(ImmutableList.<ImmutableList<RexLiteral>>of(), rowType);
  }

  /** Converts an iterable of lists into an immutable list of immutable lists
   * with the same contents. Returns the same object if possible. */
  private static <E> ImmutableList<ImmutableList<E>> copy(
      Iterable<? extends List<E>> tupleList) {
    final ImmutableList.Builder<ImmutableList<E>> builder =
        ImmutableList.builder();
    int changeCount = 0;
    for (List<E> literals : tupleList) {
      final ImmutableList<E> literals2 =
          ImmutableList.copyOf(literals);
      builder.add(literals2);
      if (literals != literals2) {
        ++changeCount;
      }
    }
    if (changeCount == 0) {
      // don't make a copy if we don't have to
      //noinspection unchecked
      return (ImmutableList<ImmutableList<E>>) tupleList;
    }
    return builder.build();
  }

  /** Creates a limit without a sort. */
  public RelBuilder limit(int offset, int fetch) {
    return sortLimit(offset, fetch, ImmutableList.of());
  }

  /** Creates an Exchange by distribution. */
  public RelBuilder exchange(RelDistribution distribution) {
    RelNode exchange = exchangeFactory.createExchange(peek(), distribution);
    replaceTop(exchange);
    return this;
  }

  /** Creates a SortExchange by distribution and collation. */
  public RelBuilder sortExchange(RelDistribution distribution,
      RelCollation collation) {
    RelNode exchange = sortExchangeFactory
        .createSortExchange(peek(), distribution, collation);
    replaceTop(exchange);
    return this;
  }

  /** Creates a {@link Sort} by field ordinals.
   *
   * <p>Negative fields mean descending: -1 means field(0) descending,
   * -2 means field(1) descending, etc.
   */
  public RelBuilder sort(int... fields) {
    final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
    for (int field : fields) {
      builder.add(field < 0 ? desc(field(-field - 1)) : field(field));
    }
    return sortLimit(-1, -1, builder.build());
  }

  /** Creates a {@link Sort} by expressions. */
  public RelBuilder sort(RexNode... nodes) {
    return sortLimit(-1, -1, ImmutableList.copyOf(nodes));
  }

  /** Creates a {@link Sort} by expressions. */
  public RelBuilder sort(Iterable<? extends RexNode> nodes) {
    return sortLimit(-1, -1, nodes);
  }

  /** Creates a {@link Sort} by expressions, with limit and offset. */
  public RelBuilder sortLimit(int offset, int fetch, RexNode... nodes) {
    return sortLimit(offset, fetch, ImmutableList.copyOf(nodes));
  }

  /** Creates a {@link Sort} by a list of expressions, with limit and offset.
   *
   * @param offset Number of rows to skip; non-positive means don't skip any
   * @param fetch Maximum number of rows to fetch; negative means no limit
   * @param nodes Sort expressions
   */
  public RelBuilder sortLimit(int offset, int fetch,
      Iterable<? extends RexNode> nodes) {
    final Registrar registrar = new Registrar(fields());
    final List<RelFieldCollation> fieldCollations =
        registrar.registerFieldCollations(nodes);

    final RexNode offsetNode = offset <= 0 ? null : literal(offset);
    final RexNode fetchNode = fetch < 0 ? null : literal(fetch);
    if (offsetNode == null && fetch == 0) {
      return empty();
    }
    if (offsetNode == null && fetchNode == null && fieldCollations.isEmpty()) {
      return this; // sort is trivial
    }

    if (fieldCollations.isEmpty()) {
      assert registrar.addedFieldCount() == 0;
      RelNode top = peek();
      if (top instanceof Sort) {
        final Sort sort2 = (Sort) top;
        if (sort2.offset == null && sort2.fetch == null) {
          replaceTop(sort2.getInput());
          final RelNode sort =
              sortFactory.createSort(peek(), sort2.collation,
                  offsetNode, fetchNode);
          replaceTop(sort);
          return this;
        }
      }
      if (top instanceof Project) {
        final Project project = (Project) top;
        if (project.getInput() instanceof Sort) {
          final Sort sort2 = (Sort) project.getInput();
          if (sort2.offset == null && sort2.fetch == null) {
            final RelNode sort =
                sortFactory.createSort(sort2.getInput(), sort2.collation,
                    offsetNode, fetchNode);
            replaceTop(
                projectFactory.createProject(sort,
                    project.getProjects(),
                    Pair.right(project.getNamedProjects())));
            return this;
          }
        }
      }
    }
    if (registrar.addedFieldCount() > 0) {
      project(registrar.extraNodes);
    }
    final RelNode sort =
        sortFactory.createSort(peek(), RelCollations.of(fieldCollations),
            offsetNode, fetchNode);
    replaceTop(sort);
    if (registrar.addedFieldCount() > 0) {
      project(registrar.originalExtraNodes);
    }
    return this;
  }

  private static RelFieldCollation collation(RexNode node,
      RelFieldCollation.Direction direction,
      RelFieldCollation.NullDirection nullDirection, List<RexNode> extraNodes) {
    switch (node.getKind()) {
    case INPUT_REF:
      return new RelFieldCollation(((RexInputRef) node).getIndex(), direction,
          Util.first(nullDirection, direction.defaultNullDirection()));
    case DESCENDING:
      return collation(((RexCall) node).getOperands().get(0),
          RelFieldCollation.Direction.DESCENDING,
          nullDirection, extraNodes);
    case NULLS_FIRST:
      return collation(((RexCall) node).getOperands().get(0), direction,
          RelFieldCollation.NullDirection.FIRST, extraNodes);
    case NULLS_LAST:
      return collation(((RexCall) node).getOperands().get(0), direction,
          RelFieldCollation.NullDirection.LAST, extraNodes);
    default:
      final int fieldIndex = extraNodes.size();
      extraNodes.add(node);
      return new RelFieldCollation(fieldIndex, direction,
          Util.first(nullDirection, direction.defaultNullDirection()));
    }
  }

  /**
   * Creates a projection that converts the current relational expression's
   * output to a desired row type.
   *
   * @param castRowType row type after cast
   * @param rename      if true, use field names from castRowType; if false,
   *                    preserve field names from rel
   */
  public RelBuilder convert(RelDataType castRowType, boolean rename) {
    final RelNode r = build();
    final RelNode r2 =
        RelOptUtil.createCastRel(r, castRowType, rename, projectFactory);
    push(r2);
    return this;
  }

  public RelBuilder permute(Mapping mapping) {
    assert mapping.getMappingType().isSingleSource();
    assert mapping.getMappingType().isMandatorySource();
    if (mapping.isIdentity()) {
      return this;
    }
    final List<RexNode> exprList = new ArrayList<>();
    for (int i = 0; i < mapping.getTargetCount(); i++) {
      exprList.add(field(mapping.getSource(i)));
    }
    return project(exprList);
  }

  public RelBuilder aggregate(GroupKey groupKey,
      List<AggregateCall> aggregateCalls) {
    return aggregate(groupKey,
        Lists.transform(aggregateCalls, AggCallImpl2::new));
  }

  /** Creates a {@link Match}. */
  public RelBuilder match(RexNode pattern, boolean strictStart,
      boolean strictEnd, Map<String, RexNode> patternDefinitions,
      Iterable<? extends RexNode> measureList, RexNode after,
      Map<String, ? extends SortedSet<String>> subsets, boolean allRows,
      Iterable<? extends RexNode> partitionKeys,
      Iterable<? extends RexNode> orderKeys, RexNode interval) {
    final Registrar registrar =
        new Registrar(fields(), peek().getRowType().getFieldNames());
    final List<RelFieldCollation> fieldCollations =
        registrar.registerFieldCollations(orderKeys);

    final ImmutableBitSet partitionBitSet =
        ImmutableBitSet.of(registrar.registerExpressions(partitionKeys));

    final RelDataTypeFactory.Builder typeBuilder = cluster.getTypeFactory().builder();
    for (RexNode partitionKey : partitionKeys) {
      typeBuilder.add(partitionKey.toString(), partitionKey.getType());
    }
    if (allRows) {
      for (RexNode orderKey : orderKeys) {
        if (!typeBuilder.nameExists(orderKey.toString())) {
          typeBuilder.add(orderKey.toString(), orderKey.getType());
        }
      }

      final RelDataType inputRowType = peek().getRowType();
      for (RelDataTypeField fs : inputRowType.getFieldList()) {
        if (!typeBuilder.nameExists(fs.getName())) {
          typeBuilder.add(fs);
        }
      }
    }

    final ImmutableMap.Builder<String, RexNode> measures = ImmutableMap.builder();
    for (RexNode measure : measureList) {
      List<RexNode> operands = ((RexCall) measure).getOperands();
      String alias = operands.get(1).toString();
      typeBuilder.add(alias, operands.get(0).getType());
      measures.put(alias, operands.get(0));
    }

    final RelNode match = matchFactory.createMatch(peek(), pattern,
        typeBuilder.build(), strictStart, strictEnd, patternDefinitions,
        measures.build(), after, subsets, allRows,
        partitionBitSet, RelCollations.of(fieldCollations),
        interval);
    stack.push(new Frame(match));
    return this;
  }

  /** Clears the stack.
   *
   * <p>The builder's state is now the same as when it was created. */
  public void clear() {
    stack.clear();
  }

  /** Information necessary to create a call to an aggregate function.
   *
   * @see RelBuilder#aggregateCall */
  public interface AggCall {
    /** Returns a copy of this AggCall that applies a filter before aggregating
     * values. */
    AggCall filter(RexNode condition);

    /** Returns a copy of this AggCall that sorts its input values by
     * {@code orderKeys} before aggregating, as in SQL's {@code WITHIN GROUP}
     * clause. */
    AggCall sort(Iterable<RexNode> orderKeys);

    /** Returns a copy of this AggCall that sorts its input values by
     * {@code orderKeys} before aggregating, as in SQL's {@code WITHIN GROUP}
     * clause. */
    AggCall sort(RexNode... orderKeys);

    /** Returns a copy of this AggCall that may return approximate results
     * if {@code approximate} is true. */
    AggCall approximate(boolean approximate);

    /** Returns a copy of this AggCall that ignores nulls. */
    AggCall ignoreNulls(boolean ignoreNulls);

    /** Returns a copy of this AggCall with a given alias. */
    AggCall as(String alias);

    /** Returns a copy of this AggCall that is optionally distinct. */
    AggCall distinct(boolean distinct);

    /** Returns a copy of this AggCall that is distinct. */
    AggCall distinct();
  }

  /** Information necessary to create the GROUP BY clause of an Aggregate.
   *
   * @see RelBuilder#groupKey */
  public interface GroupKey {
    /** Assigns an alias to this group key.
     *
     * <p>Used to assign field names in the {@code group} operation. */
    GroupKey alias(String alias);
  }

  /** Implementation of {@link RelBuilder.GroupKey}. */
  public static class GroupKeyImpl implements GroupKey {
    public final ImmutableList<RexNode> nodes;
    public final ImmutableList<ImmutableList<RexNode>> nodeLists;
    public final String alias;

    GroupKeyImpl(ImmutableList<RexNode> nodes,
        ImmutableList<ImmutableList<RexNode>> nodeLists, String alias) {
      this.nodes = Objects.requireNonNull(nodes);
      this.nodeLists = nodeLists;
      this.alias = alias;
    }

    @Override public String toString() {
      return alias == null ? nodes.toString() : nodes + " as " + alias;
    }

    public GroupKey alias(String alias) {
      return Objects.equals(this.alias, alias)
          ? this
          : new GroupKeyImpl(nodes, nodeLists, alias);
    }
  }

  /** Implementation of {@link AggCall}. */
  private class AggCallImpl implements AggCall {
    private final SqlAggFunction aggFunction;
    private final boolean distinct;
    private final boolean approximate;
    private final boolean ignoreNulls;
    private final RexNode filter; // may be null
    private final String alias; // may be null
    private final ImmutableList<RexNode> operands; // may be empty, never null
    private final ImmutableList<RexNode> orderKeys; // may be empty, never null

    AggCallImpl(SqlAggFunction aggFunction, boolean distinct,
        boolean approximate, boolean ignoreNulls, RexNode filter,
        String alias, ImmutableList<RexNode> operands,
        ImmutableList<RexNode> orderKeys) {
      this.aggFunction = Objects.requireNonNull(aggFunction);
      // If the aggregate function ignores DISTINCT,
      // make the DISTINCT flag FALSE.
      this.distinct = distinct
          && aggFunction.getDistinctOptionality() != Optionality.IGNORED;
      this.approximate = approximate;
      this.ignoreNulls = ignoreNulls;
      this.alias = alias;
      this.operands = Objects.requireNonNull(operands);
      this.orderKeys = Objects.requireNonNull(orderKeys);
      if (filter != null) {
        if (filter.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
          throw RESOURCE.filterMustBeBoolean().ex();
        }
        if (filter.getType().isNullable()) {
          filter = call(SqlStdOperatorTable.IS_TRUE, filter);
        }
      }
      this.filter = filter;
    }

    public AggCall sort(Iterable<RexNode> orderKeys) {
      final ImmutableList<RexNode> orderKeyList =
          ImmutableList.copyOf(orderKeys);
      return orderKeyList.equals(this.orderKeys)
          ? this
          : new AggCallImpl(aggFunction, distinct, approximate, ignoreNulls,
              filter, alias, operands, orderKeyList);
    }

    public AggCall sort(RexNode... orderKeys) {
      return sort(ImmutableList.copyOf(orderKeys));
    }

    public AggCall approximate(boolean approximate) {
      return approximate == this.approximate
          ? this
          : new AggCallImpl(aggFunction, distinct, approximate, ignoreNulls,
              filter, alias, operands, orderKeys);
    }

    public AggCall filter(RexNode condition) {
      return Objects.equals(condition, this.filter)
          ? this
          : new AggCallImpl(aggFunction, distinct, approximate, ignoreNulls,
              condition, alias, operands, orderKeys);
    }

    public AggCall as(String alias) {
      return Objects.equals(alias, this.alias)
          ? this
          : new AggCallImpl(aggFunction, distinct, approximate, ignoreNulls,
              filter, alias, operands, orderKeys);
    }

    public AggCall distinct(boolean distinct) {
      return distinct == this.distinct
          ? this
          : new AggCallImpl(aggFunction, distinct, approximate, ignoreNulls,
              filter, alias, operands, orderKeys);
    }

    public AggCall distinct() {
      return distinct(true);
    }

    public AggCall ignoreNulls(boolean ignoreNulls) {
      return ignoreNulls == this.ignoreNulls
          ? this
          : new AggCallImpl(aggFunction, distinct, approximate, ignoreNulls,
              filter, alias, operands, orderKeys);
    }
  }

  /** Implementation of {@link AggCall} that wraps an
   * {@link AggregateCall}. */
  private static class AggCallImpl2 implements AggCall {
    private final AggregateCall aggregateCall;

    AggCallImpl2(AggregateCall aggregateCall) {
      this.aggregateCall = Objects.requireNonNull(aggregateCall);
    }

    public AggCall sort(Iterable<RexNode> orderKeys) {
      throw new UnsupportedOperationException();
    }

    public AggCall sort(RexNode... orderKeys) {
      throw new UnsupportedOperationException();
    }

    public AggCall approximate(boolean approximate) {
      throw new UnsupportedOperationException();
    }

    public AggCall filter(RexNode condition) {
      throw new UnsupportedOperationException();
    }

    public AggCall as(String alias) {
      throw new UnsupportedOperationException();
    }

    public AggCall distinct(boolean distinct) {
      throw new UnsupportedOperationException();
    }

    public AggCall distinct() {
      throw new UnsupportedOperationException();
    }

    public AggCall ignoreNulls(boolean ignoreNulls) {
      throw new UnsupportedOperationException();
    }
  }

  /** Collects the extra expressions needed for {@link #aggregate}.
   *
   * <p>The extra expressions come from the group key and as arguments to
   * aggregate calls, and later there will be a {@link #project} or a
   * {@link #rename(List)} if necessary. */
  private static class Registrar {
    final List<RexNode> originalExtraNodes;
    final List<RexNode> extraNodes;
    final List<String> names = new ArrayList<>();

    Registrar(Iterable<RexNode> fields) {
      this(fields, ImmutableList.of());
    }

    Registrar(Iterable<RexNode> fields, List<String> fieldNames) {
      originalExtraNodes = ImmutableList.copyOf(fields);
      extraNodes = new ArrayList<>(originalExtraNodes);
      names.addAll(fieldNames);
    }

    int registerExpression(RexNode node) {
      switch (node.getKind()) {
      case AS:
        final List<RexNode> operands = ((RexCall) node).operands;
        int i = registerExpression(operands.get(0));
        names.set(i, RexLiteral.stringValue(operands.get(1)));
        return i;
      }
      int i = extraNodes.indexOf(node);
      if (i < 0) {
        i = extraNodes.size();
        extraNodes.add(node);
        names.add(null);
      }
      return i;
    }

    List<Integer> registerExpressions(Iterable<? extends RexNode> nodes) {
      final List<Integer> builder = new ArrayList<>();
      for (RexNode node : nodes) {
        builder.add(registerExpression(node));
      }
      return builder;
    }

    List<RelFieldCollation> registerFieldCollations(
        Iterable<? extends RexNode> orderKeys) {
      final List<RelFieldCollation> fieldCollations = new ArrayList<>();
      for (RexNode orderKey : orderKeys) {
        final RelFieldCollation collation =
            collation(orderKey, RelFieldCollation.Direction.ASCENDING, null,
                extraNodes);
        if (!RelCollations.ordinals(fieldCollations)
            .contains(collation.getFieldIndex())) {
          fieldCollations.add(collation);
        }
      }
      return ImmutableList.copyOf(fieldCollations);
    }

    /** Returns the number of fields added. */
    int addedFieldCount() {
      return extraNodes.size() - originalExtraNodes.size();
    }
  }

  /** Builder stack frame.
   *
   * <p>Describes a previously created relational expression and
   * information about how table aliases map into its row type. */
  private static class Frame {
    final RelNode rel;
    final ImmutableList<Field> fields;

    private Frame(RelNode rel, ImmutableList<Field> fields) {
      this.rel = rel;
      this.fields = fields;
    }

    private Frame(RelNode rel) {
      String tableAlias = deriveAlias(rel);
      ImmutableList.Builder<Field> builder = ImmutableList.builder();
      ImmutableSet<String> aliases = tableAlias == null
          ? ImmutableSet.of()
          : ImmutableSet.of(tableAlias);
      for (RelDataTypeField field : rel.getRowType().getFieldList()) {
        builder.add(new Field(aliases, field));
      }
      this.rel = rel;
      this.fields = builder.build();
    }

    private static String deriveAlias(RelNode rel) {
      if (rel instanceof TableScan) {
        final List<String> names = rel.getTable().getQualifiedName();
        if (!names.isEmpty()) {
          return Util.last(names);
        }
      }
      return null;
    }

    List<RelDataTypeField> fields() {
      return Pair.right(fields);
    }
  }

  /** A field that belongs to a stack {@link Frame}. */
  private static class Field
      extends Pair<ImmutableSet<String>, RelDataTypeField> {
    Field(ImmutableSet<String> left, RelDataTypeField right) {
      super(left, right);
    }

    Field addAlias(String alias) {
      if (left.contains(alias)) {
        return this;
      }
      final ImmutableSet<String> aliasList =
          ImmutableSet.<String>builder().addAll(left).add(alias).build();
      return new Field(aliasList, right);
    }
  }

  /** Shuttle that shifts a predicate's inputs to the left, replacing early
   * ones with references to a
   * {@link RexCorrelVariable}. */
  private class Shifter extends RexShuttle {
    private final RelNode left;
    private final CorrelationId id;
    private final RelNode right;

    Shifter(RelNode left, CorrelationId id, RelNode right) {
      this.left = left;
      this.id = id;
      this.right = right;
    }

    public RexNode visitInputRef(RexInputRef inputRef) {
      final RelDataType leftRowType = left.getRowType();
      final RexBuilder rexBuilder = getRexBuilder();
      final int leftCount = leftRowType.getFieldCount();
      if (inputRef.getIndex() < leftCount) {
        final RexNode v = rexBuilder.makeCorrel(leftRowType, id);
        return rexBuilder.makeFieldAccess(v, inputRef.getIndex());
      } else {
        return rexBuilder.makeInputRef(right, inputRef.getIndex() - leftCount);
      }
    }
  }

  /** Configuration of RelBuilder.
   *
   * <p>It is immutable, and all fields are public.
   *
   * <p>Use the {@link #DEFAULT} instance,
   * or call {@link #builder()} to create a builder then
   * {@link RelBuilder.ConfigBuilder#build()}. You can also use
   * {@link #toBuilder()} to modify a few properties of an existing Config. */
  public static class Config {
    /** Default configuration. */
    public static final Config DEFAULT =
        new Config(true, true);

    /** Whether {@link RelBuilder#aggregate} should eliminate duplicate
     * aggregate calls; default true. */
    public final boolean dedupAggregateCalls;

    /** Whether to simplify expressions; default true. */
    public final boolean simplify;

    // called only from ConfigBuilder and when creating DEFAULT;
    // parameters and fields must be in alphabetical order
    private Config(boolean dedupAggregateCalls,
        boolean simplify) {
      this.dedupAggregateCalls = dedupAggregateCalls;
      this.simplify = simplify;
    }

    /** Creates a ConfigBuilder with all properties set to their default
     * values. */
    public static ConfigBuilder builder() {
      return DEFAULT.toBuilder();
    }

    /** Creates a ConfigBuilder with properties set to the values in this
     * Config. */
    public ConfigBuilder toBuilder() {
      return new ConfigBuilder()
          .withDedupAggregateCalls(dedupAggregateCalls)
          .withSimplify(simplify);
    }
  }

  /** Creates a {@link RelBuilder.Config}. */
  public static class ConfigBuilder {
    private boolean dedupAggregateCalls;
    private boolean simplify;

    private ConfigBuilder() {
    }

    /** Creates a {@link RelBuilder.Config}. */
    public Config build() {
      return new Config(dedupAggregateCalls, simplify);
    }

    /** Sets the value that will become
     * {@link org.apache.calcite.tools.RelBuilder.Config#dedupAggregateCalls}. */
    public ConfigBuilder withDedupAggregateCalls(boolean dedupAggregateCalls) {
      this.dedupAggregateCalls = dedupAggregateCalls;
      return this;
    }

    /** Sets the value that will become
     * {@link org.apache.calcite.tools.RelBuilder.Config#simplify}. */
    public ConfigBuilder withSimplify(boolean simplify) {
      this.simplify = simplify;
      return this;
    }
  }
}

// End RelBuilder.java
