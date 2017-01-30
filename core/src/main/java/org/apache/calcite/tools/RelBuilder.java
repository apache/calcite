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
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

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
 * expressions ({@link org.apache.calcite.rel.logical.LogicalFilter},
 * {@link org.apache.calcite.rel.logical.LogicalProject} and so forth).
 * But you could override those factories so that, say, {@code filter} creates
 * instead a {@code HiveFilter}.
 *
 * <p>It is not thread-safe.
 */
public class RelBuilder {
  private static final Function<RexNode, String> FN_TYPE =
      new Function<RexNode, String>() {
        public String apply(RexNode input) {
          return input + ": " + input.getType();
        }
      };

  protected final RelOptCluster cluster;
  protected final RelOptSchema relOptSchema;
  private final RelFactories.FilterFactory filterFactory;
  private final RelFactories.ProjectFactory projectFactory;
  private final RelFactories.AggregateFactory aggregateFactory;
  private final RelFactories.SortFactory sortFactory;
  private final RelFactories.SetOpFactory setOpFactory;
  private final RelFactories.JoinFactory joinFactory;
  private final RelFactories.SemiJoinFactory semiJoinFactory;
  private final RelFactories.CorrelateFactory correlateFactory;
  private final RelFactories.ValuesFactory valuesFactory;
  private final RelFactories.TableScanFactory scanFactory;
  private final Deque<Frame> stack = new ArrayDeque<>();
  private final boolean simplify;

  protected RelBuilder(Context context, RelOptCluster cluster,
      RelOptSchema relOptSchema) {
    this.cluster = cluster;
    this.relOptSchema = relOptSchema;
    if (context == null) {
      context = Contexts.EMPTY_CONTEXT;
    }
    this.simplify = Hook.REL_BUILDER_SIMPLIFY.get(true);
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
    this.setOpFactory =
        Util.first(context.unwrap(RelFactories.SetOpFactory.class),
            RelFactories.DEFAULT_SET_OP_FACTORY);
    this.joinFactory =
        Util.first(context.unwrap(RelFactories.JoinFactory.class),
            RelFactories.DEFAULT_JOIN_FACTORY);
    this.semiJoinFactory =
        Util.first(context.unwrap(RelFactories.SemiJoinFactory.class),
            RelFactories.DEFAULT_SEMI_JOIN_FACTORY);
    this.correlateFactory =
        Util.first(context.unwrap(RelFactories.CorrelateFactory.class),
            RelFactories.DEFAULT_CORRELATE_FACTORY);
    this.valuesFactory =
        Util.first(context.unwrap(RelFactories.ValuesFactory.class),
            RelFactories.DEFAULT_VALUES_FACTORY);
    this.scanFactory =
        Util.first(context.unwrap(RelFactories.TableScanFactory.class),
            RelFactories.DEFAULT_TABLE_SCAN_FACTORY);
  }

  /** Creates a RelBuilder. */
  public static RelBuilder create(FrameworkConfig config) {
    final RelOptCluster[] clusters = {null};
    final RelOptSchema[] relOptSchemas = {null};
    Frameworks.withPrepare(
        new Frameworks.PrepareAction<Void>(config) {
          public Void apply(RelOptCluster cluster, RelOptSchema relOptSchema,
              SchemaPlus rootSchema, CalciteServerStatement statement) {
            clusters[0] = cluster;
            relOptSchemas[0] = relOptSchema;
            return null;
          }
        });
    return new RelBuilder(config.getContext(), clusters[0], relOptSchemas[0]);
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
    return new RelBuilderFactory() {
      public RelBuilder create(RelOptCluster cluster, RelOptSchema schema) {
        return new RelBuilder(context, cluster, schema);
      }
    };
  }

  /** Creates a {@link RelBuilderFactory} that uses a given set of factories. */
  public static RelBuilderFactory proto(Object... factories) {
    return proto(Contexts.of(factories));
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
      return rexBuilder.constantNull();
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
    Preconditions.checkNotNull(alias);
    Preconditions.checkNotNull(fieldName);
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
            String.format("{aliases=%s,fieldName=%s}", p.e.left,
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
  public RexNode call(SqlOperator operator, RexNode... operands) {
    return call(operator, ImmutableList.copyOf(operands));
  }

  /** Creates a call to a scalar operator. */
  private RexNode call(SqlOperator operator, List<RexNode> operandList) {
    final RexBuilder builder = cluster.getRexBuilder();
    final RelDataType type = builder.deriveReturnType(operator, operandList);
    if (type == null) {
      throw new IllegalArgumentException("cannot derive type: " + operator
          + "; operands: " + Lists.transform(operandList, FN_TYPE));
    }
    return builder.makeCall(type, operator, operandList);
  }

  /** Creates a call to a scalar operator. */
  public RexNode call(SqlOperator operator,
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
    return RexUtil.simplifyAnds(cluster.getRexBuilder(), operands);
  }

  /** Creates an OR. */
  public RexNode or(RexNode... operands) {
    return or(ImmutableList.copyOf(operands));
  }

  /** Creates an OR. */
  public RexNode or(Iterable<? extends RexNode> operands) {
    return RexUtil.composeDisjunction(cluster.getRexBuilder(), operands, false);
  }

  /** Creates a NOT. */
  public RexNode not(RexNode operand) {
    return call(SqlStdOperatorTable.NOT, operand);
  }

  /** Creates an =. */
  public RexNode equals(RexNode operand0, RexNode operand1) {
    return call(SqlStdOperatorTable.EQUALS, operand0, operand1);
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
   * @see #project
   */
  public RexNode alias(RexNode expr, String alias) {
    return call(SqlStdOperatorTable.AS, expr, literal(alias));
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
    return groupKey(ImmutableList.<RexNode>of());
  }

  /** Creates a group key. */
  public GroupKey groupKey(RexNode... nodes) {
    return groupKey(ImmutableList.copyOf(nodes));
  }

  /** Creates a group key. */
  public GroupKey groupKey(Iterable<? extends RexNode> nodes) {
    return new GroupKeyImpl(ImmutableList.copyOf(nodes), false, null, null);
  }

  /** Creates a group key with grouping sets. */
  public GroupKey groupKey(Iterable<? extends RexNode> nodes, boolean indicator,
      Iterable<? extends Iterable<? extends RexNode>> nodeLists) {
    final ImmutableList.Builder<ImmutableList<RexNode>> builder =
        ImmutableList.builder();
    for (Iterable<? extends RexNode> nodeList : nodeLists) {
      builder.add(ImmutableList.copyOf(nodeList));
    }
    return new GroupKeyImpl(ImmutableList.copyOf(nodes), indicator, builder.build(), null);
  }

  /** Creates a group key of fields identified by ordinal. */
  public GroupKey groupKey(int... fieldOrdinals) {
    return groupKey(fields(ImmutableIntList.of(fieldOrdinals)));
  }

  /** Creates a group key of fields identified by name. */
  public GroupKey groupKey(String... fieldNames) {
    return groupKey(fields(ImmutableList.copyOf(fieldNames)));
  }

  /** Creates a group key with grouping sets, both identified by field positions
   * in the underlying relational expression.
   *
   * <p>This method of creating a group key does not allow you to group on new
   * expressions, only column projections, but is efficient, especially when you
   * are coming from an existing {@link Aggregate}. */
  public GroupKey groupKey(ImmutableBitSet groupSet, boolean indicator,
      ImmutableList<ImmutableBitSet> groupSets) {
    if (groupSet.length() > peek().getRowType().getFieldCount()) {
      throw new IllegalArgumentException("out of bounds: " + groupSet);
    }
    if (groupSets == null) {
      groupSets = ImmutableList.of(groupSet);
    }
    final ImmutableList<RexNode> nodes =
        fields(ImmutableIntList.of(groupSet.toArray()));
    final List<ImmutableList<RexNode>> nodeLists =
        Lists.transform(groupSets,
            new Function<ImmutableBitSet, ImmutableList<RexNode>>() {
              public ImmutableList<RexNode> apply(ImmutableBitSet input) {
                return fields(ImmutableIntList.of(input.toArray()));
              }
            });
    return groupKey(nodes, indicator, nodeLists);
  }

  /** Creates a call to an aggregate function. */
  public AggCall aggregateCall(SqlAggFunction aggFunction, boolean distinct,
      RexNode filter, String alias, RexNode... operands) {
    return aggregateCall(aggFunction, distinct, filter, alias,
        ImmutableList.copyOf(operands));
  }

  /** Creates a call to an aggregate function. */
  public AggCall aggregateCall(SqlAggFunction aggFunction, boolean distinct,
      RexNode filter, String alias, Iterable<? extends RexNode> operands) {
    if (filter != null) {
      if (filter.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
        throw RESOURCE.filterMustBeBoolean().ex();
      }
      if (filter.getType().isNullable()) {
        filter = call(SqlStdOperatorTable.IS_TRUE, filter);
      }
    }
    return new AggCallImpl(aggFunction, distinct, filter, alias,
        ImmutableList.copyOf(operands));
  }

  /** Creates a call to the COUNT aggregate function. */
  public AggCall count(boolean distinct, String alias, RexNode... operands) {
    return aggregateCall(SqlStdOperatorTable.COUNT, distinct, null, alias,
        operands);
  }

  /** Creates a call to the COUNT(*) aggregate function. */
  public AggCall countStar(String alias) {
    return aggregateCall(SqlStdOperatorTable.COUNT, false, null, alias);
  }

  /** Creates a call to the SUM aggregate function. */
  public AggCall sum(boolean distinct, String alias, RexNode operand) {
    return aggregateCall(SqlStdOperatorTable.SUM, distinct, null, alias,
        operand);
  }

  /** Creates a call to the AVG aggregate function. */
  public AggCall avg(boolean distinct, String alias, RexNode operand) {
    return aggregateCall(
        SqlStdOperatorTable.AVG, distinct, null, alias, operand);
  }

  /** Creates a call to the MIN aggregate function. */
  public AggCall min(String alias, RexNode operand) {
    return aggregateCall(SqlStdOperatorTable.MIN, false, null, alias, operand);
  }

  /** Creates a call to the MAX aggregate function. */
  public AggCall max(String alias, RexNode operand) {
    return aggregateCall(SqlStdOperatorTable.MAX, false, null, alias, operand);
  }

  // Methods that create relational expressions

  /** Creates a {@link org.apache.calcite.rel.core.TableScan} of the table
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
      throw RESOURCE.tableNotFound(Joiner.on(".").join(names)).ex();
    }
    final RelNode scan = scanFactory.createScan(cluster, relOptTable);
    push(scan);
    return this;
  }

  /** Creates a {@link org.apache.calcite.rel.core.TableScan} of the table
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

  /** Creates a {@link org.apache.calcite.rel.core.Filter} of an array of
   * predicates.
   *
   * <p>The predicates are combined using AND,
   * and optimized in a similar way to the {@link #and} method.
   * If the result is TRUE no filter is created. */
  public RelBuilder filter(RexNode... predicates) {
    return filter(ImmutableList.copyOf(predicates));
  }

  /** Creates a {@link org.apache.calcite.rel.core.Filter} of a list of
   * predicates.
   *
   * <p>The predicates are combined using AND,
   * and optimized in a similar way to the {@link #and} method.
   * If the result is TRUE no filter is created. */
  public RelBuilder filter(Iterable<? extends RexNode> predicates) {
    final RexNode x = RexUtil.simplifyAnds(cluster.getRexBuilder(), predicates, true);
    if (x.isAlwaysFalse()) {
      return empty();
    }
    if (!x.isAlwaysTrue()) {
      final Frame frame = stack.pop();
      final RelNode filter = filterFactory.createFilter(frame.rel, x);
      stack.push(new Frame(filter, frame.fields));
    }
    return this;
  }

  /** Creates a {@link org.apache.calcite.rel.core.Project} of the given list
   * of expressions.
   *
   * <p>Infers names as would {@link #project(Iterable, Iterable)} if all
   * suggested names were null.
   *
   * @param nodes Expressions
   */
  public RelBuilder project(Iterable<? extends RexNode> nodes) {
    return project(nodes, ImmutableList.<String>of());
  }

  /** Creates a {@link org.apache.calcite.rel.core.Project} of the given list
   * of expressions and field names.
   *
   * @param nodes Expressions
   * @param fieldNames field names for expressions
   */
  public RelBuilder project(Iterable<? extends RexNode> nodes,
      Iterable<String> fieldNames) {
    return project(nodes, fieldNames, false);
  }

  /** Creates a {@link org.apache.calcite.rel.core.Project} of the given list
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
   *     {@link org.apache.calcite.sql.fun.SqlStdOperatorTable#AS}
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
    final List<String> names = new ArrayList<>();
    final List<RexNode> exprList = new ArrayList<>();
    final Iterator<String> nameIterator = fieldNames.iterator();
    for (RexNode node : nodes) {
      if (simplify) {
        node = RexUtil.simplifyPreservingType(getRexBuilder(), node);
      }
      exprList.add(node);
      String name = nameIterator.hasNext() ? nameIterator.next() : null;
      names.add(name != null ? name : inferAlias(exprList, node));
    }
    final Frame frame = stack.peek();
    final ImmutableList.Builder<Field> fields = ImmutableList.builder();
    final Set<String> uniqueNameList =
        getTypeFactory().getTypeSystem().isSchemaCaseSensitive()
        ? new HashSet<String>()
        : new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    // calculate final names and build field list
    for (int i = 0; i < names.size(); ++i) {
      RexNode node = exprList.get(i);
      String name = names.get(i);
      Field field;
      if (name == null || uniqueNameList.contains(name)) {
        int j = 0;
        if (name == null) {
          j = i;
        }
        do {
          name = SqlValidatorUtil.F_SUGGESTER.apply(name, j, j++);
        } while (uniqueNameList.contains(name));
        names.set(i, name);
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
        field = new Field(ImmutableSet.<String>of(), fieldType);
        break;
      }
      uniqueNameList.add(name);
      fields.add(field);
    }
    final RelDataType inputRowType = peek().getRowType();
    if (!force && RexUtil.isIdentity(exprList, inputRowType)) {
      if (names.equals(inputRowType.getFieldNames())) {
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
        projectFactory.createProject(frame.rel, ImmutableList.copyOf(exprList),
            names);
    stack.pop();
    stack.push(new Frame(project, fields.build()));
    return this;
  }

  /** Creates a {@link org.apache.calcite.rel.core.Project} of the given
   * expressions. */
  public RelBuilder project(RexNode... nodes) {
    return project(ImmutableList.copyOf(nodes));
  }

  /** Infers the alias of an expression.
   *
   * <p>If the expression was created by {@link #alias}, replaces the expression
   * in the project list.
   */
  private String inferAlias(List<RexNode> exprList, RexNode expr) {
    switch (expr.getKind()) {
    case INPUT_REF:
      final RexInputRef ref = (RexInputRef) expr;
      return stack.peek().fields.get(ref.getIndex()).getValue().getName();
    case CAST:
      return inferAlias(exprList, ((RexCall) expr).getOperands().get(0));
    case AS:
      final RexCall call = (RexCall) expr;
      for (;;) {
        final int i = exprList.indexOf(expr);
        if (i < 0) {
          break;
        }
        exprList.set(i, call.getOperands().get(0));
      }
      return ((NlsString) ((RexLiteral) call.getOperands().get(1)).getValue())
          .getValue();
    default:
      return null;
    }
  }

  /** Creates an {@link org.apache.calcite.rel.core.Aggregate} that makes the
   * relational expression distinct on all fields. */
  public RelBuilder distinct() {
    return aggregate(groupKey(fields()));
  }

  /** Creates an {@link org.apache.calcite.rel.core.Aggregate} with an array of
   * calls. */
  public RelBuilder aggregate(GroupKey groupKey, AggCall... aggCalls) {
    return aggregate(groupKey, ImmutableList.copyOf(aggCalls));
  }

  /** Creates an {@link org.apache.calcite.rel.core.Aggregate} with a list of
   * calls. */
  public RelBuilder aggregate(GroupKey groupKey, Iterable<AggCall> aggCalls) {
    final RelDataType inputRowType = peek().getRowType();
    final List<RexNode> extraNodes = projects(inputRowType);
    final GroupKeyImpl groupKey_ = (GroupKeyImpl) groupKey;
    final ImmutableBitSet groupSet =
        ImmutableBitSet.of(registerExpressions(extraNodes, groupKey_.nodes));
    final ImmutableList<ImmutableBitSet> groupSets;
    if (groupKey_.nodeLists != null) {
      final int sizeBefore = extraNodes.size();
      final SortedSet<ImmutableBitSet> groupSetSet =
          new TreeSet<>(ImmutableBitSet.ORDERING);
      for (ImmutableList<RexNode> nodeList : groupKey_.nodeLists) {
        final ImmutableBitSet groupSet2 =
            ImmutableBitSet.of(registerExpressions(extraNodes, nodeList));
        if (!groupSet.contains(groupSet2)) {
          throw new IllegalArgumentException("group set element " + nodeList
              + " must be a subset of group key");
        }
        groupSetSet.add(groupSet2);
      }
      groupSets = ImmutableList.copyOf(groupSetSet);
      if (extraNodes.size() > sizeBefore) {
        throw new IllegalArgumentException(
            "group sets contained expressions not in group key: "
                + extraNodes.subList(sizeBefore, extraNodes.size()));
      }
    } else {
      groupSets = ImmutableList.of(groupSet);
    }
    for (AggCall aggCall : aggCalls) {
      if (aggCall instanceof AggCallImpl) {
        final AggCallImpl aggCall1 = (AggCallImpl) aggCall;
        registerExpressions(extraNodes, aggCall1.operands);
        if (aggCall1.filter != null) {
          registerExpression(extraNodes, aggCall1.filter);
        }
      }
    }
    if (extraNodes.size() > inputRowType.getFieldCount()) {
      project(extraNodes);
    }
    final Frame frame = stack.pop();
    final RelNode r = frame.rel;
    final List<AggregateCall> aggregateCalls = new ArrayList<>();
    for (AggCall aggCall : aggCalls) {
      final AggregateCall aggregateCall;
      if (aggCall instanceof AggCallImpl) {
        final AggCallImpl aggCall1 = (AggCallImpl) aggCall;
        final List<Integer> args = registerExpressions(extraNodes, aggCall1.operands);
        final int filterArg = aggCall1.filter == null ? -1
            : registerExpression(extraNodes, aggCall1.filter);
        aggregateCall =
            AggregateCall.create(aggCall1.aggFunction, aggCall1.distinct, args,
                filterArg, groupSet.cardinality(), r, null, aggCall1.alias);
      } else {
        aggregateCall = ((AggCallImpl2) aggCall).aggregateCall;
      }
      aggregateCalls.add(aggregateCall);
    }

    assert ImmutableBitSet.ORDERING.isStrictlyOrdered(groupSets) : groupSets;
    for (ImmutableBitSet set : groupSets) {
      assert groupSet.contains(set);
    }
    RelNode aggregate = aggregateFactory.createAggregate(r,
        groupKey_.indicator, groupSet, groupSets, aggregateCalls);

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
        fields.add(frame.fields.get(((RexInputRef) node).getIndex()));
        break;
      default:
        String name = aggregateFields.get(i).getName();
        RelDataTypeField fieldType =
            new RelDataTypeFieldImpl(name, i, node.getType());
        fields.add(new Field(ImmutableSet.<String>of(), fieldType));
        break;
      }
      i++;
    }
    // second, indicator fields (copy from aggregate rel type)
    if (groupKey_.indicator) {
      for (int j = 0; j < groupSet.cardinality(); ++j) {
        final RelDataTypeField field = aggregateFields.get(i);
        final RelDataTypeField fieldType =
            new RelDataTypeFieldImpl(field.getName(), i, field.getType());
        fields.add(new Field(ImmutableSet.<String>of(), fieldType));
        i++;
      }
    }
    // third, aggregate fields. retain `i' as field index
    for (int j = 0; j < aggregateCalls.size(); ++j) {
      final AggregateCall call = aggregateCalls.get(j);
      final RelDataTypeField fieldType =
          new RelDataTypeFieldImpl(aggregateFields.get(i + j).getName(), i + j,
              call.getType());
      fields.add(new Field(ImmutableSet.<String>of(), fieldType));
    }
    stack.push(new Frame(aggregate, fields.build()));
    return this;
  }

  private List<RexNode> projects(RelDataType inputRowType) {
    final List<RexNode> exprList = new ArrayList<>();
    for (RelDataTypeField field : inputRowType.getFieldList()) {
      final RexBuilder rexBuilder = cluster.getRexBuilder();
      exprList.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
    }
    return exprList;
  }

  private static int registerExpression(List<RexNode> exprList, RexNode node) {
    int i = exprList.indexOf(node);
    if (i < 0) {
      i = exprList.size();
      exprList.add(node);
    }
    return i;
  }

  private static List<Integer> registerExpressions(List<RexNode> extraNodes,
      Iterable<? extends RexNode> nodes) {
    final List<Integer> builder = new ArrayList<>();
    for (RexNode node : nodes) {
      builder.add(registerExpression(extraNodes, node));
    }
    return builder;
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

  /** Creates a {@link org.apache.calcite.rel.core.Union} of the two most recent
   * relational expressions on the stack.
   *
   * @param all Whether to create UNION ALL
   */
  public RelBuilder union(boolean all) {
    return union(all, 2);
  }

  /** Creates a {@link org.apache.calcite.rel.core.Union} of the {@code n}
   * most recent relational expressions on the stack.
   *
   * @param all Whether to create UNION ALL
   * @param n Number of inputs to the UNION operator
   */
  public RelBuilder union(boolean all, int n) {
    return setOp(all, SqlKind.UNION, n);
  }

  /** Creates an {@link org.apache.calcite.rel.core.Intersect} of the two most
   * recent relational expressions on the stack.
   *
   * @param all Whether to create INTERSECT ALL
   */
  public RelBuilder intersect(boolean all) {
    return intersect(all, 2);
  }

  /** Creates an {@link org.apache.calcite.rel.core.Intersect} of the {@code n}
   * most recent relational expressions on the stack.
   *
   * @param all Whether to create INTERSECT ALL
   * @param n Number of inputs to the INTERSECT operator
   */
  public RelBuilder intersect(boolean all, int n) {
    return setOp(all, SqlKind.INTERSECT, n);
  }

  /** Creates a {@link org.apache.calcite.rel.core.Minus} of the two most recent
   * relational expressions on the stack.
   *
   * @param all Whether to create EXCEPT ALL
   */
  public RelBuilder minus(boolean all) {
    return minus(all, 2);
  }

  /** Creates a {@link org.apache.calcite.rel.core.Minus} of the {@code n}
   * most recent relational expressions on the stack.
   *
   * @param all Whether to create EXCEPT ALL
   */
  public RelBuilder minus(boolean all, int n) {
    return setOp(all, SqlKind.EXCEPT, n);
  }

  /** Creates a {@link org.apache.calcite.rel.core.Join}. */
  public RelBuilder join(JoinRelType joinType, RexNode condition0,
      RexNode... conditions) {
    return join(joinType, Lists.asList(condition0, conditions));
  }

  /** Creates a {@link org.apache.calcite.rel.core.Join} with multiple
   * conditions. */
  public RelBuilder join(JoinRelType joinType,
      Iterable<? extends RexNode> conditions) {
    return join(joinType, and(conditions),
        ImmutableSet.<CorrelationId>of());
  }

  public RelBuilder join(JoinRelType joinType, RexNode condition) {
    return join(joinType, condition, ImmutableSet.<CorrelationId>of());
  }

  /** Creates a {@link org.apache.calcite.rel.core.Join} with correlating
   * variables. */
  public RelBuilder join(JoinRelType joinType, RexNode condition,
      Set<CorrelationId> variablesSet) {
    Frame right = stack.pop();
    final Frame left = stack.pop();
    final RelNode join;
    final boolean correlate = variablesSet.size() == 1;
    RexNode postCondition = literal(true);
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
          requiredColumns, SemiJoinType.of(joinType));
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

  /** Creates a {@link org.apache.calcite.rel.core.Join} using USING syntax.
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

  /** Creates a {@link org.apache.calcite.rel.core.SemiJoin}. */
  public RelBuilder semiJoin(Iterable<? extends RexNode> conditions) {
    final Frame right = stack.pop();
    final RelNode semiJoin =
        semiJoinFactory.createSemiJoin(peek(), right.rel, and(conditions));
    replaceTop(semiJoin);
    return this;
  }

  /** Creates a {@link org.apache.calcite.rel.core.SemiJoin}. */
  public RelBuilder semiJoin(RexNode... conditions) {
    return semiJoin(ImmutableList.copyOf(conditions));
  }

  /** Assigns a table alias to the top entry on the stack. */
  public RelBuilder as(final String alias) {
    final Frame pair = stack.pop();
    List<Field> newFields =
        Lists.transform(pair.fields, new Function<Field, Field>() {
          public Field apply(Field field) {
            return new Field(ImmutableSet.<String>builder().addAll(field.left)
                .add(alias).build(), field.right);
          }
        });
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
    final RelDataTypeFactory.FieldInfoBuilder rowTypeBuilder =
        cluster.getTypeFactory().builder();
    for (final Ord<String> fieldName : Ord.zip(fieldNames)) {
      final String name =
          fieldName.e != null ? fieldName.e : "expr$" + fieldName.i;
      final RelDataType type = cluster.getTypeFactory().leastRestrictive(
          new AbstractList<RelDataType>() {
            public RelDataType get(int index) {
              return tupleList.get(index).get(fieldName.i).getType();
            }

            public int size() {
              return rowCount;
            }
          });
      rowTypeBuilder.add(name, type);
    }
    final RelDataType rowType = rowTypeBuilder.build();
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
   * specified row type as the input, and ignores the input entirely.
   * But schema-on-query systems such as Drill might override this method to
   * create a relation expression that retains the input, just to read its
   * schema.
   */
  public RelBuilder empty() {
    final Frame frame = stack.pop();
    return values(frame.rel.getRowType());
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
  private static <E> ImmutableList<ImmutableList<E>>
  copy(Iterable<? extends List<E>> tupleList) {
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
    return sortLimit(offset, fetch, ImmutableList.<RexNode>of());
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
    final List<RelFieldCollation> fieldCollations = new ArrayList<>();
    final RelDataType inputRowType = peek().getRowType();
    final List<RexNode> extraNodes = projects(inputRowType);
    final List<RexNode> originalExtraNodes = ImmutableList.copyOf(extraNodes);
    for (RexNode node : nodes) {
      fieldCollations.add(
          collation(node, RelFieldCollation.Direction.ASCENDING, null,
              extraNodes));
    }
    final RexNode offsetNode = offset <= 0 ? null : literal(offset);
    final RexNode fetchNode = fetch < 0 ? null : literal(fetch);
    if (offsetNode == null && fetch == 0) {
      return empty();
    }
    if (offsetNode == null && fetchNode == null && fieldCollations.isEmpty()) {
      return this; // sort is trivial
    }

    final boolean addedFields = extraNodes.size() > originalExtraNodes.size();
    if (fieldCollations.isEmpty()) {
      assert !addedFields;
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
    if (addedFields) {
      project(extraNodes);
    }
    final RelNode sort =
        sortFactory.createSort(peek(), RelCollations.of(fieldCollations),
            offsetNode, fetchNode);
    replaceTop(sort);
    if (addedFields) {
      project(originalExtraNodes);
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
    final List<RexNode> exprList = Lists.newArrayList();
    for (int i = 0; i < mapping.getTargetCount(); i++) {
      exprList.add(field(mapping.getSource(i)));
    }
    return project(exprList);
  }

  public RelBuilder aggregate(GroupKey groupKey,
      List<AggregateCall> aggregateCalls) {
    return aggregate(groupKey,
        Lists.transform(
            aggregateCalls, new Function<AggregateCall, AggCall>() {
              public AggCall apply(AggregateCall input) {
                return new AggCallImpl2(input);
              }
            }));
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
  protected static class GroupKeyImpl implements GroupKey {
    final ImmutableList<RexNode> nodes;
    final boolean indicator;
    final ImmutableList<ImmutableList<RexNode>> nodeLists;
    final String alias;

    GroupKeyImpl(ImmutableList<RexNode> nodes, boolean indicator,
        ImmutableList<ImmutableList<RexNode>> nodeLists, String alias) {
      this.nodes = Preconditions.checkNotNull(nodes);
      this.indicator = indicator;
      this.nodeLists = nodeLists;
      this.alias = alias;
    }

    @Override public String toString() {
      return alias == null ? nodes.toString() : nodes + " as " + alias;
    }

    public GroupKey alias(String alias) {
      return Objects.equals(this.alias, alias)
          ? this
          : new GroupKeyImpl(nodes, indicator, nodeLists, alias);
    }
  }

  /** Implementation of {@link RelBuilder.AggCall}. */
  private static class AggCallImpl implements AggCall {
    private final SqlAggFunction aggFunction;
    private final boolean distinct;
    private final RexNode filter;
    private final String alias;
    private final ImmutableList<RexNode> operands;

    AggCallImpl(SqlAggFunction aggFunction, boolean distinct, RexNode filter,
        String alias, ImmutableList<RexNode> operands) {
      this.aggFunction = aggFunction;
      this.distinct = distinct;
      this.filter = filter;
      this.alias = alias;
      this.operands = operands;
    }
  }

  /** Implementation of {@link RelBuilder.AggCall} that wraps an
   * {@link AggregateCall}. */
  private static class AggCallImpl2 implements AggCall {
    private final AggregateCall aggregateCall;

    AggCallImpl2(AggregateCall aggregateCall) {
      this.aggregateCall = Preconditions.checkNotNull(aggregateCall);
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
          ? ImmutableSet.<String>of()
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
    public Field(ImmutableSet<String> left, RelDataTypeField right) {
      super(left, right);
    }
  }

  /** Shuttle that shifts a predicate's inputs to the left, replacing early
   * ones with references to a
   * {@link org.apache.calcite.rex.RexCorrelVariable}. */
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
}

// End RelBuilder.java
