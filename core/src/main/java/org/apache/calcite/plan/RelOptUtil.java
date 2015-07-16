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
package org.apache.calcite.plan;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.externalize.RelXmlWriter;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexMultisetUtil;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Permutation;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * <code>RelOptUtil</code> defines static utility methods for use in optimizing
 * {@link RelNode}s.
 */
public abstract class RelOptUtil {
  //~ Static fields/initializers ---------------------------------------------

  public static final double EPSILON = 1.0e-5;

  /** Predicate for whether a filter contains multisets or windowed
   * aggregates. */
  public static final Predicate<Filter> FILTER_PREDICATE =
      new Predicate<Filter>() {
        public boolean apply(Filter filter) {
          return !(B
              && RexMultisetUtil.containsMultiset(filter.getCondition(), true)
              || RexOver.containsOver(filter.getCondition()));
        }
      };

  /** Predicate for whether a project contains multisets or windowed
   * aggregates. */
  public static final Predicate<Project> PROJECT_PREDICATE =
      new Predicate<Project>() {
        public boolean apply(Project project) {
          return !(B
              && RexMultisetUtil.containsMultiset(project.getProjects(), true)
              || RexOver.containsOver(project.getProjects(), null));
        }
      };

  /** Predicate for whether a calc contains multisets or windowed
   * aggregates. */
  public static final Predicate<Calc> CALC_PREDICATE =
      new Predicate<Calc>() {
        public boolean apply(Calc calc) {
          return !(B
              && RexMultisetUtil.containsMultiset(calc.getProgram())
              || calc.getProgram().containsAggs());
        }
      };

  static final boolean B = false;

  private static final Function<RelDataTypeField, RelDataType> GET_TYPE =
      new Function<RelDataTypeField, RelDataType>() {
        public RelDataType apply(RelDataTypeField field) {
          return field.getType();
        }
      };

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns a list of variables set by a relational expression or its
   * descendants.
   */
  public static Set<String> getVariablesSet(RelNode rel) {
    VariableSetVisitor visitor = new VariableSetVisitor();
    go(visitor, rel);
    return visitor.variables;
  }

  /**
   * Returns a set of distinct variables set by <code>rel0</code> and used by
   * <code>rel1</code>.
   */
  public static List<String> getVariablesSetAndUsed(
      RelNode rel0,
      RelNode rel1) {
    Set<String> set = getVariablesSet(rel0);
    if (set.size() == 0) {
      return ImmutableList.of();
    }
    Set<String> used = getVariablesUsed(rel1);
    if (used.size() == 0) {
      return ImmutableList.of();
    }
    final List<String> result = new ArrayList<>();
    for (String s : set) {
      if (used.contains(s) && !result.contains(s)) {
        result.add(s);
      }
    }
    return result;
  }

  /**
   * Returns a set of variables used by a relational expression or its
   * descendants. The set may contain duplicates. The item type is the same as
   * {@link org.apache.calcite.rex.RexVariable#getName}
   */
  public static Set<String> getVariablesUsed(RelNode rel) {
    final VariableUsedVisitor vuv = new VariableUsedVisitor();
    RelShuttle visitor = new RelHomogeneousShuttle() {
      @Override public RelNode visit(RelNode other) {
        other.collectVariablesUsed(vuv.variables);
        other.accept(vuv);
        RelNode result = super.visit(other);
        // Important! Remove stopped variables AFTER we visit
        // children. (which what super.visit() does)
        vuv.variables.removeAll(other.getVariablesStopped());
        return result;
      }
    };
    rel.accept(visitor);
    return vuv.variables;
  }

  /**
   * Sets a {@link RelVisitor} going on a given relational expression, and
   * returns the result.
   */
  public static void go(
      RelVisitor visitor,
      RelNode p) {
    try {
      visitor.go(p);
    } catch (Throwable e) {
      throw Util.newInternal(e, "while visiting tree");
    }
  }

  /**
   * Returns a list of the types of the fields in a given struct type. The
   * list is immutable.
   *
   * @param type Struct type
   * @return List of field types
   * @see org.apache.calcite.rel.type.RelDataType#getFieldNames()
   */
  public static List<RelDataType> getFieldTypeList(final RelDataType type) {
    return Lists.transform(type.getFieldList(), GET_TYPE);
  }

  public static boolean areRowTypesEqual(
      RelDataType rowType1,
      RelDataType rowType2,
      boolean compareNames) {
    if (rowType1 == rowType2) {
      return true;
    }
    if (compareNames) {
      // if types are not identity-equal, then either the names or
      // the types must be different
      return false;
    }
    if (rowType2.getFieldCount() != rowType1.getFieldCount()) {
      return false;
    }
    final List<RelDataTypeField> f1 = rowType1.getFieldList();
    final List<RelDataTypeField> f2 = rowType2.getFieldList();
    for (Pair<RelDataTypeField, RelDataTypeField> pair : Pair.zip(f1, f2)) {
      final RelDataType type1 = pair.left.getType();
      final RelDataType type2 = pair.right.getType();
      // If one of the types is ANY comparison should succeed
      if (type1.getSqlTypeName() == SqlTypeName.ANY
        || type2.getSqlTypeName() == SqlTypeName.ANY) {
        continue;
      }
      if (!type1.equals(type2)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Verifies that a row type being added to an equivalence class matches the
   * existing type, raising an assertion if this is not the case.
   *
   * @param originalRel      canonical rel for equivalence class
   * @param newRel           rel being added to equivalence class
   * @param equivalenceClass object representing equivalence class
   */
  public static void verifyTypeEquivalence(
      RelNode originalRel,
      RelNode newRel,
      Object equivalenceClass) {
    RelDataType expectedRowType = originalRel.getRowType();
    RelDataType actualRowType = newRel.getRowType();

    // Row types must be the same, except for field names.
    if (areRowTypesEqual(expectedRowType, actualRowType, false)) {
      return;
    }

    String s = "Cannot add expression of different type to set:\n"
        + "set type is " + expectedRowType.getFullTypeString()
        + "\nexpression type is " + actualRowType.getFullTypeString()
        + "\nset is " + equivalenceClass.toString()
        + "\nexpression is " + newRel.toString();
    throw Util.newInternal(s);
  }

  /**
   * Returns a permutation describing where output fields come from. In
   * the returned map, value of {@code map.getTargetOpt(i)} is {@code n} if
   * field {@code i} projects input field {@code n}, -1 if it is an
   * expression.
   */
  public static Mappings.TargetMapping permutation(
      List<RexNode> nodes,
      RelDataType inputRowType) {
    final Mappings.TargetMapping mapping =
        Mappings.create(
            MappingType.PARTIAL_FUNCTION,
            nodes.size(),
            inputRowType.getFieldCount());
    for (Ord<RexNode> node : Ord.zip(nodes)) {
      if (node.e instanceof RexInputRef) {
        mapping.set(
            node.i,
            ((RexInputRef) node.e).getIndex());
      } else if (node.e.isA(SqlKind.CAST)) {
        RexNode operand = ((RexCall) node.e).getOperands().get(0);
        if (operand instanceof RexInputRef) {
          mapping.set(
              node.i,
              ((RexInputRef) operand).getIndex());
        }
      }
    }
    return mapping;
  }

  /**
   * Creates a plan suitable for use in <code>EXISTS</code> or <code>IN</code>
   * statements.
   *
   * <p>See {@link org.apache.calcite.sql2rel.SqlToRelConverter#convertExists}
   *
   * <p>Note: this implementation of createExistsPlan is only called from
   * net.sf.farrago.fennel.rel. The last two arguments do not apply to those
   * invocations and can be removed from the method.
   *
   * @param cluster    Cluster
   * @param seekRel    A query rel, for example the resulting rel from 'select *
   *                   from emp' or 'values (1,2,3)' or '('Foo', 34)'.
   * @param conditions May be null
   * @param extraExpr  Column expression to add. "TRUE" for EXISTS and IN
   * @param extraName  Name of expression to add.
   * @return relational expression which outer joins a boolean condition
   * column
   */
  public static RelNode createExistsPlan(
      RelOptCluster cluster,
      RelNode seekRel,
      List<RexNode> conditions,
      RexLiteral extraExpr,
      String extraName) {
    assert extraExpr == null || extraName != null;
    RelNode ret = seekRel;

    if ((conditions != null) && (conditions.size() > 0)) {
      RexNode conditionExp =
          RexUtil.composeConjunction(
              cluster.getRexBuilder(), conditions, true);

      ret = createFilter(ret, conditionExp,
          RelFactories.DEFAULT_FILTER_FACTORY);
    }

    if (extraExpr != null) {
      RexBuilder rexBuilder = cluster.getRexBuilder();
      RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

      assert extraExpr == rexBuilder.makeLiteral(true);

      // this should only be called for the exists case
      // first stick an Agg on top of the subquery
      // agg does not like no agg functions so just pretend it is
      // doing a min(TRUE)

      ret = createProject(ret, ImmutableList.of(extraExpr), null);
      final List<RelDataType> argTypes =
          ImmutableList.of(
              typeFactory.createSqlType(SqlTypeName.BOOLEAN));

      SqlAggFunction minFunction =
          new SqlMinMaxAggFunction(
              argTypes,
              true,
              SqlMinMaxAggFunction.MINMAX_COMPARABLE);

      final AggregateCall aggCall =
          AggregateCall.create(minFunction,
              false,
              ImmutableList.of(0),
              0,
              ret,
              null,
              extraName);

      ret =
          LogicalAggregate.create(ret, false,
              ImmutableBitSet.of(), null, ImmutableList.of(aggCall));
    }

    return ret;
  }

  /**
   * Creates a plan suitable for use in <code>EXISTS</code> or <code>IN</code>
   * statements.
   *
   * @see org.apache.calcite.sql2rel.SqlToRelConverter#convertExists
   *
   * @param seekRel    A query rel, for example the resulting rel from 'select *
   *                   from emp' or 'values (1,2,3)' or '('Foo', 34)'.
   * @param subqueryType Sub-query type
   * @param logic  Whether to use 2- or 3-valued boolean logic
   * @param needsOuterJoin Whether query needs outer join
   *
   * @return A pair of a relational expression which outer joins a boolean
   * condition column, and a numeric offset. The offset is 2 if column 0 is
   * the number of rows and column 1 is the number of rows with not-null keys;
   * 0 otherwise.
   */
  public static Pair<RelNode, Boolean> createExistsPlan(
      RelNode seekRel,
      SubqueryType subqueryType,
      Logic logic,
      boolean needsOuterJoin) {
    switch (subqueryType) {
    case SCALAR:
      return Pair.of(seekRel, false);
    default:
      RelNode ret = seekRel;
      final RelOptCluster cluster = seekRel.getCluster();
      final RexBuilder rexBuilder = cluster.getRexBuilder();
      final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

      final int keyCount = ret.getRowType().getFieldCount();
      if (!needsOuterJoin) {
        return Pair.<RelNode, Boolean>of(
            LogicalAggregate.create(ret, false,
                ImmutableBitSet.range(keyCount), null,
                ImmutableList.<AggregateCall>of()),
            false);
      }

      // for IN/NOT IN, it needs to output the fields
      final List<RexNode> exprs = new ArrayList<>();
      if (subqueryType == SubqueryType.IN) {
        for (int i = 0; i < keyCount; i++) {
          exprs.add(rexBuilder.makeInputRef(ret, i));
        }
      }

      final int projectedKeyCount = exprs.size();
      exprs.add(rexBuilder.makeLiteral(true));

      ret = createProject(ret, exprs, null);

      final List<RelDataType> argTypes =
          ImmutableList.of(typeFactory.createSqlType(SqlTypeName.BOOLEAN));

      final SqlAggFunction minFunction =
          new SqlMinMaxAggFunction(argTypes, true,
              SqlMinMaxAggFunction.MINMAX_COMPARABLE);

      final AggregateCall aggCall =
          AggregateCall.create(minFunction,
              false,
              ImmutableList.of(projectedKeyCount),
              projectedKeyCount,
              ret,
              null,
              null);

      ret = LogicalAggregate.create(ret, false,
          ImmutableBitSet.range(projectedKeyCount), null,
          ImmutableList.of(aggCall));

      switch (logic) {
      case TRUE_FALSE_UNKNOWN:
      case UNKNOWN_AS_TRUE:
        return Pair.of(ret, true);
      default:
        return Pair.of(ret, false);
      }
    }
  }

  /**
   * Creates a LogicalProject which accomplishes a rename.
   *
   * @param outputType a row type descriptor whose field names the generated
   *                   LogicalProject must match
   * @param rel        the rel whose output is to be renamed; rel.getRowType()
   *                   must be the same as outputType except for field names
   * @return generated relational expression
   */
  public static RelNode createRenameRel(
      RelDataType outputType,
      RelNode rel) {
    RelDataType inputType = rel.getRowType();
    List<RelDataTypeField> inputFields = inputType.getFieldList();
    int n = inputFields.size();

    List<RelDataTypeField> outputFields = outputType.getFieldList();
    assert outputFields.size() == n
        : "rename: field count mismatch: in=" + inputType
        + ", out" + outputType;

    final List<Pair<RexNode, String>> renames = new ArrayList<>();
    for (Pair<RelDataTypeField, RelDataTypeField> pair
        : Pair.zip(inputFields, outputFields)) {
      final RelDataTypeField inputField = pair.left;
      final RelDataTypeField outputField = pair.right;
      assert inputField.getType().equals(outputField.getType());
      final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
      renames.add(
          Pair.<RexNode, String>of(
              rexBuilder.makeInputRef(inputField.getType(),
                  inputField.getIndex()),
              outputField.getName()));
    }
    return createProject(rel, Pair.left(renames), Pair.right(renames));
  }

  /**
   * Creates a relational expression which filters according to a given
   * condition, returning the same fields as its input, using the default
   * filter factory.
   *
   * @param child     Child relational expression
   * @param condition Condition
   * @return Relational expression
   */
  public static RelNode createFilter(RelNode child, RexNode condition) {
    return createFilter(child, condition, RelFactories.DEFAULT_FILTER_FACTORY);
  }

  /**
   * Creates a relational expression which filters according to a given
   * condition, returning the same fields as its input.
   *
   * @param child     Child relational expression
   * @param condition Condition
   * @param filterFactory Filter factory
   * @return Relational expression
   */
  public static RelNode createFilter(RelNode child, RexNode condition,
      RelFactories.FilterFactory filterFactory) {
    return filterFactory.createFilter(child, condition);
  }

  /** Creates a filter, using the default filter factory,
   * or returns the original relational expression if the
   * condition is trivial. */
  public static RelNode createFilter(RelNode child,
      Iterable<? extends RexNode> conditions) {
    return createFilter(child, conditions, RelFactories.DEFAULT_FILTER_FACTORY);
  }

  /** Creates a filter using the default factory,
   * or returns the original relational expression if the
   * condition is trivial. */
  public static RelNode createFilter(RelNode child,
      Iterable<? extends RexNode> conditions,
      RelFactories.FilterFactory filterFactory) {
    final RelOptCluster cluster = child.getCluster();
    final RexNode condition =
        RexUtil.composeConjunction(cluster.getRexBuilder(), conditions, true);
    if (condition == null) {
      return child;
    } else {
      return createFilter(child, condition, filterFactory);
    }
  }

  /**
   * Creates a filter which will remove rows containing NULL values.
   *
   * @param rel           the rel to be filtered
   * @param fieldOrdinals array of 0-based field ordinals to filter, or null
   *                      for all fields
   * @return filtered rel
   */
  public static RelNode createNullFilter(
      RelNode rel,
      Integer[] fieldOrdinals) {
    RexNode condition = null;
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    RelDataType rowType = rel.getRowType();
    int n;
    if (fieldOrdinals != null) {
      n = fieldOrdinals.length;
    } else {
      n = rowType.getFieldCount();
    }
    List<RelDataTypeField> fields = rowType.getFieldList();
    for (int i = 0; i < n; ++i) {
      int iField;
      if (fieldOrdinals != null) {
        iField = fieldOrdinals[i];
      } else {
        iField = i;
      }
      RelDataType type = fields.get(iField).getType();
      if (!type.isNullable()) {
        continue;
      }
      RexNode newCondition =
          rexBuilder.makeCall(
              SqlStdOperatorTable.IS_NOT_NULL,
              rexBuilder.makeInputRef(type, iField));
      if (condition == null) {
        condition = newCondition;
      } else {
        condition =
            rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                condition,
                newCondition);
      }
    }
    if (condition == null) {
      // no filtering required
      return rel;
    }

    return createFilter(rel, condition, RelFactories.DEFAULT_FILTER_FACTORY);
  }

  /**
   * Creates a projection which casts a rel's output to a desired row type.
   *
   * @param rel         producer of rows to be converted
   * @param castRowType row type after cast
   * @param rename      if true, use field names from castRowType; if false,
   *                    preserve field names from rel
   * @return conversion rel
   */
  public static RelNode createCastRel(
      final RelNode rel,
      RelDataType castRowType,
      boolean rename) {
    return createCastRel(
        rel, castRowType, rename, RelFactories.DEFAULT_PROJECT_FACTORY);
  }

  /**
   * Creates a projection which casts a rel's output to a desired row type.
   *
   * @param rel         producer of rows to be converted
   * @param castRowType row type after cast
   * @param rename      if true, use field names from castRowType; if false,
   *                    preserve field names from rel
   * @param projectFactory Project Factory
   * @return conversion rel
   */
  public static RelNode createCastRel(
      final RelNode rel,
      RelDataType castRowType,
      boolean rename,
      RelFactories.ProjectFactory projectFactory) {
    assert projectFactory != null;
    RelDataType rowType = rel.getRowType();
    if (areRowTypesEqual(rowType, castRowType, rename)) {
      // nothing to do
      return rel;
    }
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    final List<RexNode> castExps =
        RexUtil.generateCastExpressions(rexBuilder, castRowType, rowType);
    if (rename) {
      // Use names and types from castRowType.
      return projectFactory.createProject(
          rel,
          castExps,
          castRowType.getFieldNames());
    } else {
      // Use names from rowType, types from castRowType.
      return projectFactory.createProject(
          rel,
          castExps,
          rowType.getFieldNames());
    }
  }

  /**
   * Creates a LogicalAggregate that removes all duplicates from the result of
   * an underlying relational expression.
   *
   * @param rel underlying rel
   * @return rel implementing SingleValueAgg
   */
  public static RelNode createSingleValueAggRel(
      RelOptCluster cluster,
      RelNode rel) {
    // assert (rel.getRowType().getFieldCount() == 1);
    final int aggCallCnt = rel.getRowType().getFieldCount();
    final List<AggregateCall> aggCalls = new ArrayList<>();

    for (int i = 0; i < aggCallCnt; i++) {
      aggCalls.add(
          AggregateCall.create(
              SqlStdOperatorTable.SINGLE_VALUE, false, ImmutableList.of(i), -1,
              0, rel, null, null));
    }

    return LogicalAggregate.create(rel, false,
        ImmutableBitSet.of(), null, aggCalls);
  }

  /**
   * Creates a LogicalAggregate that removes all duplicates from the result of
   * an underlying relational expression.
   *
   * @param rel underlying rel
   * @return rel implementing DISTINCT
   */
  public static RelNode createDistinctRel(RelNode rel) {
    return LogicalAggregate.create(rel, false,
        ImmutableBitSet.range(rel.getRowType().getFieldCount()), null,
        ImmutableList.<AggregateCall>of());
  }

  public static boolean analyzeSimpleEquiJoin(
      LogicalJoin join,
      int[] joinFieldOrdinals) {
    RexNode joinExp = join.getCondition();
    if (joinExp.getKind() != SqlKind.EQUALS) {
      return false;
    }
    RexCall binaryExpression = (RexCall) joinExp;
    RexNode leftComparand = binaryExpression.operands.get(0);
    RexNode rightComparand = binaryExpression.operands.get(1);
    if (!(leftComparand instanceof RexInputRef)) {
      return false;
    }
    if (!(rightComparand instanceof RexInputRef)) {
      return false;
    }

    final int leftFieldCount =
        join.getLeft().getRowType().getFieldCount();
    RexInputRef leftFieldAccess = (RexInputRef) leftComparand;
    if (!(leftFieldAccess.getIndex() < leftFieldCount)) {
      // left field must access left side of join
      return false;
    }

    RexInputRef rightFieldAccess = (RexInputRef) rightComparand;
    if (!(rightFieldAccess.getIndex() >= leftFieldCount)) {
      // right field must access right side of join
      return false;
    }

    joinFieldOrdinals[0] = leftFieldAccess.getIndex();
    joinFieldOrdinals[1] = rightFieldAccess.getIndex() - leftFieldCount;
    return true;
  }

  /**
   * Splits out the equi-join components of a join condition, and returns
   * what's left. For example, given the condition
   *
   * <blockquote><code>L.A = R.X AND L.B = L.C AND (L.D = 5 OR L.E =
   * R.Y)</code></blockquote>
   *
   * returns
   *
   * <ul>
   * <li>leftKeys = {A}
   * <li>rightKeys = {X}
   * <li>rest = L.B = L.C AND (L.D = 5 OR L.E = R.Y)</li>
   * </ul>
   *
   * @param left      left input to join
   * @param right     right input to join
   * @param condition join condition
   * @param leftKeys  The ordinals of the fields from the left input which are
   *                  equi-join keys
   * @param rightKeys The ordinals of the fields from the right input which
   *                  are equi-join keys
   * @return remaining join filters that are not equijoins; may return a
   * {@link RexLiteral} true, but never null
   */
  public static RexNode splitJoinCondition(
      RelNode left,
      RelNode right,
      RexNode condition,
      List<Integer> leftKeys,
      List<Integer> rightKeys) {
    final List<RexNode> nonEquiList = new ArrayList<>();

    splitJoinCondition(
        left.getRowType().getFieldCount(),
        condition,
        leftKeys,
        rightKeys,
        nonEquiList);

    return RexUtil.composeConjunction(
        left.getCluster().getRexBuilder(), nonEquiList, false);
  }

  /**
   * Returns whether a join condition is an "equi-join" condition.
   *
   * @param left      Left input of join
   * @param right     Right input of join
   * @param condition Condition
   * @return Whether condition is equi-join
   */
  public static boolean isEqui(
      RelNode left,
      RelNode right,
      RexNode condition) {
    final List<Integer> leftKeys = new ArrayList<>();
    final List<Integer> rightKeys = new ArrayList<>();
    final List<RexNode> nonEquiList = new ArrayList<>();
    splitJoinCondition(
        left.getRowType().getFieldCount(),
        condition,
        leftKeys,
        rightKeys,
        nonEquiList);
    return nonEquiList.size() == 0;
  }

  /**
   * Splits out the equi-join (and optionally, a single non-equi) components
   * of a join condition, and returns what's left. Projection might be
   * required by the caller to provide join keys that are not direct field
   * references.
   *
   * @param sysFieldList  list of system fields
   * @param leftRel       left join input
   * @param rightRel      right join input
   * @param condition     join condition
   * @param leftJoinKeys  The join keys from the left input which are equi-join
   *                      keys
   * @param rightJoinKeys The join keys from the right input which are
   *                      equi-join keys
   * @param filterNulls   The join key positions for which null values will not
   *                      match. null values only match for the "is not distinct
   *                      from" condition.
   * @param rangeOp       if null, only locate equi-joins; otherwise, locate a
   *                      single non-equi join predicate and return its operator
   *                      in this list; join keys associated with the non-equi
   *                      join predicate are at the end of the key lists
   *                      returned
   * @return What's left, never null
   */
  public static RexNode splitJoinCondition(
      List<RelDataTypeField> sysFieldList,
      RelNode leftRel,
      RelNode rightRel,
      RexNode condition,
      List<RexNode> leftJoinKeys,
      List<RexNode> rightJoinKeys,
      List<Integer> filterNulls,
      List<SqlOperator> rangeOp) {
    return splitJoinCondition(
        sysFieldList,
        ImmutableList.of(leftRel, rightRel),
        condition,
        ImmutableList.of(leftJoinKeys, rightJoinKeys),
        filterNulls,
        rangeOp);
  }

  /**
   * Splits out the equi-join (and optionally, a single non-equi) components
   * of a join condition, and returns what's left. Projection might be
   * required by the caller to provide join keys that are not direct field
   * references.
   *
   * @param sysFieldList  list of system fields
   * @param inputs        join inputs
   * @param condition     join condition
   * @param joinKeys      The join keys from the inputs which are equi-join
   *                      keys
   * @param filterNulls   The join key positions for which null values will not
   *                      match. null values only match for the "is not distinct
   *                      from" condition.
   * @param rangeOp       if null, only locate equi-joins; otherwise, locate a
   *                      single non-equi join predicate and return its operator
   *                      in this list; join keys associated with the non-equi
   *                      join predicate are at the end of the key lists
   *                      returned
   * @return What's left, never null
   */
  public static RexNode splitJoinCondition(
      List<RelDataTypeField> sysFieldList,
      List<RelNode> inputs,
      RexNode condition,
      List<List<RexNode>> joinKeys,
      List<Integer> filterNulls,
      List<SqlOperator> rangeOp) {
    final List<RexNode> nonEquiList = new ArrayList<>();

    splitJoinCondition(
        sysFieldList,
        inputs,
        condition,
        joinKeys,
        filterNulls,
        rangeOp,
        nonEquiList);

    // Convert the remainders into a list that are AND'ed together.
    return RexUtil.composeConjunction(
        inputs.get(0).getCluster().getRexBuilder(), nonEquiList, false);
  }

  public static RexNode splitCorrelatedFilterCondition(
      LogicalFilter filter,
      List<RexInputRef> joinKeys,
      List<RexNode> correlatedJoinKeys) {
    final List<RexNode> nonEquiList = new ArrayList<>();

    splitCorrelatedFilterCondition(
        filter,
        filter.getCondition(),
        joinKeys,
        correlatedJoinKeys,
        nonEquiList);

    // Convert the remainders into a list that are AND'ed together.
    return RexUtil.composeConjunction(
        filter.getCluster().getRexBuilder(), nonEquiList, true);
  }

  public static RexNode splitCorrelatedFilterCondition(
      LogicalFilter filter,
      List<RexNode> joinKeys,
      List<RexNode> correlatedJoinKeys,
      boolean extractCorrelatedFieldAccess) {
    final List<RexNode> nonEquiList = new ArrayList<>();

    splitCorrelatedFilterCondition(
        filter,
        filter.getCondition(),
        joinKeys,
        correlatedJoinKeys,
        nonEquiList,
        extractCorrelatedFieldAccess);

    // Convert the remainders into a list that are AND'ed together.
    return RexUtil.composeConjunction(
        filter.getCluster().getRexBuilder(), nonEquiList, true);
  }

  private static void splitJoinCondition(
      List<RelDataTypeField> sysFieldList,
      List<RelNode> inputs,
      RexNode condition,
      List<List<RexNode>> joinKeys,
      List<Integer> filterNulls,
      List<SqlOperator> rangeOp,
      List<RexNode> nonEquiList) {
    final int sysFieldCount = sysFieldList.size();
    final RelOptCluster cluster = inputs.get(0).getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    final ImmutableBitSet[] inputsRange = new ImmutableBitSet[inputs.size()];
    int totalFieldCount = 0;
    for (int i = 0; i < inputs.size(); i++) {
      final int firstField = totalFieldCount + sysFieldCount;
      totalFieldCount = firstField + inputs.get(i).getRowType().getFieldCount();
      inputsRange[i] = ImmutableBitSet.range(firstField, totalFieldCount);
    }

    // adjustment array
    int[] adjustments = new int[totalFieldCount];
    for (int i = 0; i < inputs.size(); i++) {
      final int adjustment = inputsRange[i].nextSetBit(0);
      for (int j = adjustment; j < inputsRange[i].length(); j++) {
        adjustments[j] = -adjustment;
      }
    }

    if (condition instanceof RexCall) {
      RexCall call = (RexCall) condition;
      if (call.getOperator() == SqlStdOperatorTable.AND) {
        for (RexNode operand : call.getOperands()) {
          splitJoinCondition(
              sysFieldList,
              inputs,
              operand,
              joinKeys,
              filterNulls,
              rangeOp,
              nonEquiList);
        }
        return;
      }

      RexNode leftKey = null;
      RexNode rightKey = null;
      int leftInput = 0;
      int rightInput = 0;
      List<RelDataTypeField> leftFields = null;
      List<RelDataTypeField> rightFields = null;
      boolean reverse = false;

      SqlKind kind = call.getKind();

      // Only consider range operators if we haven't already seen one
      if ((kind == SqlKind.EQUALS)
          || (filterNulls != null
          && kind == SqlKind.IS_NOT_DISTINCT_FROM)
          || (rangeOp != null
          && rangeOp.isEmpty()
          && (kind == SqlKind.GREATER_THAN
          || kind == SqlKind.GREATER_THAN_OR_EQUAL
          || kind == SqlKind.LESS_THAN
          || kind == SqlKind.LESS_THAN_OR_EQUAL))) {
        final List<RexNode> operands = call.getOperands();
        RexNode op0 = operands.get(0);
        RexNode op1 = operands.get(1);

        final ImmutableBitSet projRefs0 = InputFinder.bits(op0);
        final ImmutableBitSet projRefs1 = InputFinder.bits(op1);

        boolean foundBothInputs = false;
        for (int i = 0; i < inputs.size() && !foundBothInputs; i++) {
          if (projRefs0.intersects(inputsRange[i])
                  && projRefs0.union(inputsRange[i]).equals(inputsRange[i])) {
            if (leftKey == null) {
              leftKey = op0;
              leftInput = i;
              leftFields = inputs.get(leftInput).getRowType().getFieldList();
            } else {
              rightKey = op0;
              rightInput = i;
              rightFields = inputs.get(rightInput).getRowType().getFieldList();
              reverse = true;
              foundBothInputs = true;
            }
          } else if (projRefs1.intersects(inputsRange[i])
                  && projRefs1.union(inputsRange[i]).equals(inputsRange[i])) {
            if (leftKey == null) {
              leftKey = op1;
              leftInput = i;
              leftFields = inputs.get(leftInput).getRowType().getFieldList();
            } else {
              rightKey = op1;
              rightInput = i;
              rightFields = inputs.get(rightInput).getRowType().getFieldList();
              foundBothInputs = true;
            }
          }
        }

        if ((leftKey != null) && (rightKey != null)) {
          // replace right Key input ref
          rightKey =
              rightKey.accept(
                  new RelOptUtil.RexInputConverter(
                      rexBuilder,
                      rightFields,
                      rightFields,
                      adjustments));

          // left key only needs to be adjusted if there are system
          // fields, but do it for uniformity
          leftKey =
              leftKey.accept(
                  new RelOptUtil.RexInputConverter(
                      rexBuilder,
                      leftFields,
                      leftFields,
                      adjustments));

          RelDataType leftKeyType = leftKey.getType();
          RelDataType rightKeyType = rightKey.getType();

          if (leftKeyType != rightKeyType) {
            // perform casting
            RelDataType targetKeyType =
                typeFactory.leastRestrictive(
                    ImmutableList.of(leftKeyType, rightKeyType));

            if (targetKeyType == null) {
              throw Util.newInternal(
                  "Cannot find common type for join keys "
                  + leftKey + " (type " + leftKeyType + ") and "
                  + rightKey + " (type " + rightKeyType + ")");
            }

            if (leftKeyType != targetKeyType) {
              leftKey =
                  rexBuilder.makeCast(targetKeyType, leftKey);
            }

            if (rightKeyType != targetKeyType) {
              rightKey =
                  rexBuilder.makeCast(targetKeyType, rightKey);
            }
          }
        }
      }

      if ((rangeOp == null)
          && ((leftKey == null) || (rightKey == null))) {
        // no equality join keys found yet:
        // try transforming the condition to
        // equality "join" conditions, e.g.
        //     f(LHS) > 0 ===> ( f(LHS) > 0 ) = TRUE,
        // and make the RHS produce TRUE, but only if we're strictly
        // looking for equi-joins
        final ImmutableBitSet projRefs = InputFinder.bits(condition);
        leftKey = null;
        rightKey = null;

        boolean foundInput = false;
        for (int i = 0; i < inputs.size() && !foundInput; i++) {
          final int lowerLimit = inputsRange[i].nextSetBit(0);
          final int upperLimit = inputsRange[i].length();
          if (projRefs.nextSetBit(lowerLimit) < upperLimit) {
            leftInput = i;
            leftFields = inputs.get(leftInput).getRowType().getFieldList();

            leftKey = condition.accept(
                new RelOptUtil.RexInputConverter(
                    rexBuilder,
                    leftFields,
                    leftFields,
                    adjustments));

            rightKey = rexBuilder.makeLiteral(true);

            // effectively performing an equality comparison
            kind = SqlKind.EQUALS;

            foundInput = true;
          }
        }
      }

      if ((leftKey != null) && (rightKey != null)) {
        // found suitable join keys
        // add them to key list, ensuring that if there is a
        // non-equi join predicate, it appears at the end of the
        // key list; also mark the null filtering property
        addJoinKey(
            joinKeys.get(leftInput),
            leftKey,
            (rangeOp != null) && !rangeOp.isEmpty());
        addJoinKey(
            joinKeys.get(rightInput),
            rightKey,
            (rangeOp != null) && !rangeOp.isEmpty());
        if (filterNulls != null
            && kind == SqlKind.EQUALS) {
          // nulls are considered not matching for equality comparison
          // add the position of the most recently inserted key
          filterNulls.add(joinKeys.get(leftInput).size() - 1);
        }
        if (rangeOp != null
            && kind != SqlKind.EQUALS
            && kind != SqlKind.IS_DISTINCT_FROM) {
          if (reverse) {
            kind = reverse(kind);
          }
          rangeOp.add(op(kind, call.getOperator()));
        }
        return;
      } // else fall through and add this condition as nonEqui condition
    }

    // The operator is not of RexCall type
    // So we fail. Fall through.
    // Add this condition to the list of non-equi-join conditions.
    nonEquiList.add(condition);
  }

  /** Builds an equi-join condition from a set of left and right keys. */
  public static RexNode createEquiJoinCondition(
      final RelNode left, final List<Integer> leftKeys,
      final RelNode right, final List<Integer> rightKeys,
      final RexBuilder rexBuilder) {
    final List<RelDataType> leftTypes =
        RelOptUtil.getFieldTypeList(left.getRowType());
    final List<RelDataType> rightTypes =
        RelOptUtil.getFieldTypeList(right.getRowType());
    return RexUtil.composeConjunction(rexBuilder,
        new AbstractList<RexNode>() {
          @Override public RexNode get(int index) {
            final int leftKey = leftKeys.get(index);
            final int rightKey = rightKeys.get(index);
            return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(leftTypes.get(leftKey), leftKey),
                rexBuilder.makeInputRef(rightTypes.get(rightKey),
                    leftTypes.size() + rightKey));
          }

          @Override public int size() {
            return leftKeys.size();
          }
        },
        false);
  }

  public static SqlKind reverse(SqlKind kind) {
    switch (kind) {
    case GREATER_THAN:
      return SqlKind.LESS_THAN;
    case GREATER_THAN_OR_EQUAL:
      return SqlKind.LESS_THAN_OR_EQUAL;
    case LESS_THAN:
      return SqlKind.GREATER_THAN;
    case LESS_THAN_OR_EQUAL:
      return SqlKind.GREATER_THAN_OR_EQUAL;
    default:
      return kind;
    }
  }

  public static SqlOperator op(SqlKind kind, SqlOperator operator) {
    switch (kind) {
    case EQUALS:
      return SqlStdOperatorTable.EQUALS;
    case NOT_EQUALS:
      return SqlStdOperatorTable.NOT_EQUALS;
    case GREATER_THAN:
      return SqlStdOperatorTable.GREATER_THAN;
    case GREATER_THAN_OR_EQUAL:
      return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
    case LESS_THAN:
      return SqlStdOperatorTable.LESS_THAN;
    case LESS_THAN_OR_EQUAL:
      return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
    case IS_DISTINCT_FROM:
      return SqlStdOperatorTable.IS_DISTINCT_FROM;
    case IS_NOT_DISTINCT_FROM:
      return SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
    default:
      return operator;
    }
  }

  private static void addJoinKey(
      List<RexNode> joinKeyList,
      RexNode key,
      boolean preserveLastElementInList) {
    if (!joinKeyList.isEmpty() && preserveLastElementInList) {
      joinKeyList.add(joinKeyList.size() - 1, key);
    } else {
      joinKeyList.add(key);
    }
  }

  private static void splitCorrelatedFilterCondition(
      LogicalFilter filter,
      RexNode condition,
      List<RexInputRef> joinKeys,
      List<RexNode> correlatedJoinKeys,
      List<RexNode> nonEquiList) {
    if (condition instanceof RexCall) {
      RexCall call = (RexCall) condition;
      if (call.getOperator() == SqlStdOperatorTable.AND) {
        for (RexNode operand : call.getOperands()) {
          splitCorrelatedFilterCondition(
              filter,
              operand,
              joinKeys,
              correlatedJoinKeys,
              nonEquiList);
        }
        return;
      }

      if (call.getOperator() == SqlStdOperatorTable.EQUALS) {
        final List<RexNode> operands = call.getOperands();
        RexNode op0 = operands.get(0);
        RexNode op1 = operands.get(1);

        if (!(RexUtil.containsInputRef(op0))
            && (op1 instanceof RexInputRef)) {
          correlatedJoinKeys.add(op0);
          joinKeys.add((RexInputRef) op1);
          return;
        } else if (
            (op0 instanceof RexInputRef)
                && !(RexUtil.containsInputRef(op1))) {
          joinKeys.add((RexInputRef) op0);
          correlatedJoinKeys.add(op1);
          return;
        }
      }
    }

    // The operator is not of RexCall type
    // So we fail. Fall through.
    // Add this condition to the list of non-equi-join conditions.
    nonEquiList.add(condition);
  }

  private static void splitCorrelatedFilterCondition(
      LogicalFilter filter,
      RexNode condition,
      List<RexNode> joinKeys,
      List<RexNode> correlatedJoinKeys,
      List<RexNode> nonEquiList,
      boolean extractCorrelatedFieldAccess) {
    if (condition instanceof RexCall) {
      RexCall call = (RexCall) condition;
      if (call.getOperator() == SqlStdOperatorTable.AND) {
        for (RexNode operand : call.getOperands()) {
          splitCorrelatedFilterCondition(
              filter,
              operand,
              joinKeys,
              correlatedJoinKeys,
              nonEquiList,
              extractCorrelatedFieldAccess);
        }
        return;
      }

      if (call.getOperator() == SqlStdOperatorTable.EQUALS) {
        final List<RexNode> operands = call.getOperands();
        RexNode op0 = operands.get(0);
        RexNode op1 = operands.get(1);

        if (extractCorrelatedFieldAccess) {
          if (!RexUtil.containsFieldAccess(op0)
              && (op1 instanceof RexFieldAccess)) {
            joinKeys.add(op0);
            correlatedJoinKeys.add(op1);
            return;
          } else if (
              (op0 instanceof RexFieldAccess)
                  && !RexUtil.containsFieldAccess(op1)) {
            correlatedJoinKeys.add(op0);
            joinKeys.add(op1);
            return;
          }
        } else {
          if (!(RexUtil.containsInputRef(op0))
              && (op1 instanceof RexInputRef)) {
            correlatedJoinKeys.add(op0);
            joinKeys.add(op1);
            return;
          } else if (
              (op0 instanceof RexInputRef)
                  && !(RexUtil.containsInputRef(op1))) {
            joinKeys.add(op0);
            correlatedJoinKeys.add(op1);
            return;
          }
        }
      }
    }

    // The operator is not of RexCall type
    // So we fail. Fall through.
    // Add this condition to the list of non-equi-join conditions.
    nonEquiList.add(condition);
  }

  private static void splitJoinCondition(
      final int leftFieldCount,
      RexNode condition,
      List<Integer> leftKeys,
      List<Integer> rightKeys,
      List<RexNode> nonEquiList) {
    if (condition instanceof RexCall) {
      RexCall call = (RexCall) condition;
      final SqlOperator operator = call.getOperator();
      if (operator == SqlStdOperatorTable.AND) {
        for (RexNode operand : call.getOperands()) {
          splitJoinCondition(
              leftFieldCount,
              operand,
              leftKeys,
              rightKeys,
              nonEquiList);
        }
        return;
      }

      // "=" and "IS NOT DISTINCT FROM" are the same except for how they
      // treat nulls. TODO: record null treatment
      if (operator == SqlStdOperatorTable.EQUALS
          || operator == SqlStdOperatorTable.IS_NOT_DISTINCT_FROM) {
        final List<RexNode> operands = call.getOperands();
        if ((operands.get(0) instanceof RexInputRef)
            && (operands.get(1) instanceof RexInputRef)) {
          RexInputRef op0 = (RexInputRef) operands.get(0);
          RexInputRef op1 = (RexInputRef) operands.get(1);

          RexInputRef leftField;
          RexInputRef rightField;
          if ((op0.getIndex() < leftFieldCount)
              && (op1.getIndex() >= leftFieldCount)) {
            // Arguments were of form 'op0 = op1'
            leftField = op0;
            rightField = op1;
          } else if (
              (op1.getIndex() < leftFieldCount)
                  && (op0.getIndex() >= leftFieldCount)) {
            // Arguments were of form 'op1 = op0'
            leftField = op1;
            rightField = op0;
          } else {
            nonEquiList.add(condition);
            return;
          }

          leftKeys.add(leftField.getIndex());
          rightKeys.add(rightField.getIndex() - leftFieldCount);
          return;
        }
        // Arguments were not field references, one from each side, so
        // we fail. Fall through.
      }
    }

    // Add this condition to the list of non-equi-join conditions.
    if (!condition.isAlwaysTrue()) {
      nonEquiList.add(condition);
    }
  }

  /**
   * Adding projection to the inputs of a join to produce the required join
   * keys.
   *
   * @param inputRels      inputs to a join
   * @param leftJoinKeys   expressions for LHS of join key
   * @param rightJoinKeys  expressions for RHS of join key
   * @param systemColCount number of system columns, usually zero. These
   *                       columns are projected at the leading edge of the
   *                       output row.
   * @param leftKeys       on return this contains the join key positions from
   *                       the new project rel on the LHS.
   * @param rightKeys      on return this contains the join key positions from
   *                       the new project rel on the RHS.
   * @param outputProj     on return this contains the positions of the original
   *                       join output in the (to be formed by caller)
   *                       LhxJoinRel. Caller needs to be responsible for adding
   *                       projection on the new join output.
   */
  public static void projectJoinInputs(
      RelNode[] inputRels,
      List<RexNode> leftJoinKeys,
      List<RexNode> rightJoinKeys,
      int systemColCount,
      List<Integer> leftKeys,
      List<Integer> rightKeys,
      List<Integer> outputProj) {
    RelNode leftRel = inputRels[0];
    RelNode rightRel = inputRels[1];
    RexBuilder rexBuilder = leftRel.getCluster().getRexBuilder();

    int origLeftInputSize = leftRel.getRowType().getFieldCount();
    int origRightInputSize = rightRel.getRowType().getFieldCount();

    final List<RexNode> newLeftFields = new ArrayList<>();
    final List<String> newLeftFieldNames = new ArrayList<>();

    final List<RexNode> newRightFields = new ArrayList<>();
    final List<String> newRightFieldNames = new ArrayList<>();
    int leftKeyCount = leftJoinKeys.size();
    int rightKeyCount = rightJoinKeys.size();
    int i;

    for (i = 0; i < systemColCount; i++) {
      outputProj.add(i);
    }

    for (i = 0; i < origLeftInputSize; i++) {
      final RelDataTypeField field =
          leftRel.getRowType().getFieldList().get(i);
      newLeftFields.add(rexBuilder.makeInputRef(field.getType(), i));
      newLeftFieldNames.add(field.getName());
      outputProj.add(systemColCount + i);
    }

    int newLeftKeyCount = 0;
    for (i = 0; i < leftKeyCount; i++) {
      RexNode leftKey = leftJoinKeys.get(i);

      if (leftKey instanceof RexInputRef) {
        // already added to the projected left fields
        // only need to remember the index in the join key list
        leftKeys.add(((RexInputRef) leftKey).getIndex());
      } else {
        newLeftFields.add(leftKey);
        newLeftFieldNames.add(null);
        leftKeys.add(origLeftInputSize + newLeftKeyCount);
        newLeftKeyCount++;
      }
    }

    int leftFieldCount = origLeftInputSize + newLeftKeyCount;
    for (i = 0; i < origRightInputSize; i++) {
      final RelDataTypeField field =
          rightRel.getRowType().getFieldList().get(i);
      newRightFields.add(rexBuilder.makeInputRef(field.getType(), i));
      newRightFieldNames.add(field.getName());
      outputProj.add(systemColCount + leftFieldCount + i);
    }

    int newRightKeyCount = 0;
    for (i = 0; i < rightKeyCount; i++) {
      RexNode rightKey = rightJoinKeys.get(i);

      if (rightKey instanceof RexInputRef) {
        // already added to the projected left fields
        // only need to remember the index in the join key list
        rightKeys.add(((RexInputRef) rightKey).getIndex());
      } else {
        newRightFields.add(rightKey);
        newRightFieldNames.add(null);
        rightKeys.add(origRightInputSize + newRightKeyCount);
        newRightKeyCount++;
      }
    }

    // added project if need to produce new keys than the original input
    // fields
    if (newLeftKeyCount > 0) {
      leftRel = createProject(leftRel, newLeftFields,
          SqlValidatorUtil.uniquify(newLeftFieldNames));
    }

    if (newRightKeyCount > 0) {
      rightRel = createProject(rightRel, newRightFields,
          SqlValidatorUtil.uniquify(newRightFieldNames));
    }

    inputRels[0] = leftRel;
    inputRels[1] = rightRel;
  }

  /**
   * Creates a projection on top of a join, if the desired projection is a
   * subset of the join columns
   *
   * @param outputProj desired projection; if null, return original join node
   * @param joinRel    the join node
   * @return projected join node or the original join if projection is
   * unnecessary
   */
  public static RelNode createProjectJoinRel(
      List<Integer> outputProj,
      RelNode joinRel) {
    int newProjectOutputSize = outputProj.size();
    List<RelDataTypeField> joinOutputFields =
        joinRel.getRowType().getFieldList();

    // If no projection was passed in, or the number of desired projection
    // columns is the same as the number of columns returned from the
    // join, then no need to create a projection
    if ((newProjectOutputSize > 0)
        && (newProjectOutputSize < joinOutputFields.size())) {
      final List<Pair<RexNode, String>> newProjects = new ArrayList<>();
      RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
      for (int fieldIndex : outputProj) {
        final RelDataTypeField field = joinOutputFields.get(fieldIndex);
        newProjects.add(
            Pair.<RexNode, String>of(
                rexBuilder.makeInputRef(field.getType(), fieldIndex),
                field.getName()));
      }

      // Create a project rel on the output of the join.
      return createProject(
          joinRel,
          Pair.left(newProjects),
          Pair.right(newProjects));
    }

    return joinRel;
  }

  public static void registerAbstractRels(RelOptPlanner planner) {
    planner.addRule(AggregateProjectPullUpConstantsRule.INSTANCE);
    planner.addRule(PruneEmptyRules.UNION_INSTANCE);
    planner.addRule(PruneEmptyRules.PROJECT_INSTANCE);
    planner.addRule(PruneEmptyRules.FILTER_INSTANCE);
    planner.addRule(PruneEmptyRules.SORT_INSTANCE);
    planner.addRule(PruneEmptyRules.AGGREGATE_INSTANCE);
    planner.addRule(PruneEmptyRules.JOIN_LEFT_INSTANCE);
    planner.addRule(PruneEmptyRules.JOIN_RIGHT_INSTANCE);
    planner.addRule(PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE);
    planner.addRule(ProjectToWindowRule.PROJECT);
    planner.addRule(FilterMergeRule.INSTANCE);
  }

  /**
   * Dumps a plan as a string.
   *
   * @param header      Header to print before the plan. Ignored if the format
   *                    is XML.
   * @param rel         Relational expression to explain.
   * @param asXml       Whether to format as XML.
   * @param detailLevel Detail level.
   * @return Plan
   */
  public static String dumpPlan(
      String header,
      RelNode rel,
      boolean asXml,
      SqlExplainLevel detailLevel) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    if (!header.equals("")) {
      pw.println(header);
    }
    RelWriter planWriter;
    if (asXml) {
      planWriter = new RelXmlWriter(pw, detailLevel);
    } else {
      planWriter = new RelWriterImpl(pw, detailLevel, false);
    }
    rel.explain(planWriter);
    pw.flush();
    return sw.toString();
  }

  /**
   * Creates the row type descriptor for the result of a DML operation, which
   * is a single column named ROWCOUNT of type BIGINT for INSERT;
   * a single column named PLAN for EXPLAIN.
   *
   * @param kind        Kind of node
   * @param typeFactory factory to use for creating type descriptor
   * @return created type
   */
  public static RelDataType createDmlRowType(
      SqlKind kind,
      RelDataTypeFactory typeFactory) {
    switch (kind) {
    case INSERT:
      return typeFactory.createStructType(
          ImmutableList.of(
              Pair.of(
                  "ROWCOUNT",
                  typeFactory.createSqlType(SqlTypeName.BIGINT))));
    case EXPLAIN:
      return typeFactory.createStructType(
          ImmutableList.of(
              Pair.of(
                  "PLAN",
                  typeFactory.createSqlType(
                      SqlTypeName.VARCHAR,
                      RelDataType.PRECISION_NOT_SPECIFIED))));
    default:
      throw Util.unexpected(kind);
    }
  }

  /**
   * Returns whether two types are equal using '='.
   *
   * @param desc1 Description of first type
   * @param type1 First type
   * @param desc2 Description of second type
   * @param type2 Second type
   * @param fail  Whether to assert if they are not equal
   * @return Whether the types are equal
   */
  public static boolean eq(
      final String desc1,
      RelDataType type1,
      final String desc2,
      RelDataType type2,
      boolean fail) {
    // if any one of the types is ANY return true
    if (type1.getSqlTypeName() == SqlTypeName.ANY
        || type2.getSqlTypeName() == SqlTypeName.ANY) {
      return true;
    }

    if (type1 != type2) {
      assert !fail : "type mismatch:\n"
          + desc1 + ":\n"
          + type1.getFullTypeString() + "\n"
          + desc2 + ":\n"
          + type2.getFullTypeString();
      return false;
    }
    return true;
  }

  /**
   * Returns whether two types are equal using
   * {@link #areRowTypesEqual(RelDataType, RelDataType, boolean)}. Both types
   * must not be null.
   *
   * @param desc1 Description of role of first type
   * @param type1 First type
   * @param desc2 Description of role of second type
   * @param type2 Second type
   * @param fail  Whether to assert if they are not equal
   * @return Whether the types are equal
   */
  public static boolean equal(
      final String desc1,
      RelDataType type1,
      final String desc2,
      RelDataType type2,
      boolean fail) {
    if (!areRowTypesEqual(type1, type2, false)) {
      if (fail) {
        throw new AssertionError(
            "Type mismatch:\n"
            + desc1 + ":\n"
            + type1.getFullTypeString() + "\n"
            + desc2 + ":\n"
            + type2.getFullTypeString());
      }
      return false;
    }
    return true;
  }

  /** Returns whether two relational expressions have the same row-type. */
  public static boolean equalType(String desc0, RelNode rel0, String desc1,
      RelNode rel1, boolean fail) {
    // TODO: change 'equal' to 'eq', which is stronger.
    return equal(desc0, rel0.getRowType(), desc1, rel1.getRowType(), fail);
  }

  /**
   * Returns a translation of the <code>IS DISTINCT FROM</code> (or <code>IS
   * NOT DISTINCT FROM</code>) sql operator.
   *
   * @param neg if false, returns a translation of IS NOT DISTINCT FROM
   */
  public static RexNode isDistinctFrom(
      RexBuilder rexBuilder,
      RexNode x,
      RexNode y,
      boolean neg) {
    RexNode ret = null;
    if (x.getType().isStruct()) {
      assert y.getType().isStruct();
      List<RelDataTypeField> xFields = x.getType().getFieldList();
      List<RelDataTypeField> yFields = y.getType().getFieldList();
      assert xFields.size() == yFields.size();
      for (Pair<RelDataTypeField, RelDataTypeField> pair
          : Pair.zip(xFields, yFields)) {
        RelDataTypeField xField = pair.left;
        RelDataTypeField yField = pair.right;
        RexNode newX =
            rexBuilder.makeFieldAccess(
                x,
                xField.getIndex());
        RexNode newY =
            rexBuilder.makeFieldAccess(
                y,
                yField.getIndex());
        RexNode newCall =
            isDistinctFromInternal(rexBuilder, newX, newY, neg);
        if (ret == null) {
          ret = newCall;
        } else {
          ret =
              rexBuilder.makeCall(
                  SqlStdOperatorTable.AND,
                  ret,
                  newCall);
        }
      }
    } else {
      ret = isDistinctFromInternal(rexBuilder, x, y, neg);
    }

    // The result of IS DISTINCT FROM is NOT NULL because it can
    // only return TRUE or FALSE.
    ret =
        rexBuilder.makeCast(
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN),
            ret);

    return ret;
  }

  private static RexNode isDistinctFromInternal(
      RexBuilder rexBuilder,
      RexNode x,
      RexNode y,
      boolean neg) {
    SqlOperator nullOp;
    SqlOperator eqOp;
    if (neg) {
      nullOp = SqlStdOperatorTable.IS_NULL;
      eqOp = SqlStdOperatorTable.EQUALS;
    } else {
      nullOp = SqlStdOperatorTable.IS_NOT_NULL;
      eqOp = SqlStdOperatorTable.NOT_EQUALS;
    }
    RexNode[] whenThenElse = {
        // when x is null
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, x),

        // then return y is [not] null
        rexBuilder.makeCall(nullOp, y),

        // when y is null
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, y),

        // then return x is [not] null
        rexBuilder.makeCall(nullOp, x),

        // else return x compared to y
        rexBuilder.makeCall(eqOp, x, y)
    };
    return rexBuilder.makeCall(
        SqlStdOperatorTable.CASE,
        whenThenElse);
  }

  /**
   * Converts a relational expression to a string, showing just basic
   * attributes.
   */
  public static String toString(final RelNode rel) {
    return toString(rel, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
  }

  /**
   * Converts a relational expression to a string.
   */
  public static String toString(
      final RelNode rel,
      SqlExplainLevel detailLevel) {
    if (rel == null) {
      return null;
    }
    final StringWriter sw = new StringWriter();
    final RelWriter planWriter =
        new RelWriterImpl(
            new PrintWriter(sw), detailLevel, false);
    rel.explain(planWriter);
    return sw.toString();
  }

  /**
   * Renames a relational expression to make its field names the same as
   * another row type. If the row type is already identical, or if the row
   * type is too different (the fields are different in number or type) does
   * nothing.
   *
   * @param rel            Relational expression
   * @param desiredRowType Desired row type (including desired field names)
   * @return Renamed relational expression, or the original expression if
   * there is nothing to do or nothing we <em>can</em> do.
   */
  public static RelNode renameIfNecessary(
      RelNode rel,
      RelDataType desiredRowType) {
    final RelDataType rowType = rel.getRowType();
    if (rowType == desiredRowType) {
      // Nothing to do.
      return rel;
    }
    assert !rowType.equals(desiredRowType);

    if (!areRowTypesEqual(rowType, desiredRowType, false)) {
      // The row types are different ignoring names. Nothing we can do.
      return rel;
    }
    rel =
        createRename(
            rel,
            desiredRowType.getFieldNames());
    return rel;
  }

  public static String dumpType(RelDataType type) {
    final StringWriter sw = new StringWriter();
    final PrintWriter pw = new PrintWriter(sw);
    final TypeDumper typeDumper = new TypeDumper(pw);
    if (type.isStruct()) {
      typeDumper.acceptFields(type.getFieldList());
    } else {
      typeDumper.accept(type);
    }
    pw.flush();
    return sw.toString();
  }

  /**
   * Decomposes a predicate into a list of expressions that are AND'ed
   * together.
   *
   * @param rexPredicate predicate to be analyzed
   * @param rexList      list of decomposed RexNodes
   */
  public static void decomposeConjunction(
      RexNode rexPredicate,
      List<RexNode> rexList) {
    if (rexPredicate == null || rexPredicate.isAlwaysTrue()) {
      return;
    }
    if (rexPredicate.isA(SqlKind.AND)) {
      for (RexNode operand : ((RexCall) rexPredicate).getOperands()) {
        decomposeConjunction(operand, rexList);
      }
    } else {
      rexList.add(rexPredicate);
    }
  }

  /**
   * Decomposes a predicate into a list of expressions that are AND'ed
   * together, and a list of expressions that are preceded by NOT.
   *
   * <p>For example, {@code a AND NOT b AND NOT (c and d) AND TRUE AND NOT
   * FALSE} returns {@code rexList = [a], notList = [b, c AND d]}.</p>
   *
   * <p>TRUE and NOT FALSE expressions are ignored. FALSE and NOT TRUE
   * expressions are placed on {@code rexList} and {@code notList} as other
   * expressions.</p>
   *
   * <p>For example, {@code a AND TRUE AND NOT TRUE} returns
   * {@code rexList = [a], notList = [TRUE]}.</p>
   *
   * @param rexPredicate predicate to be analyzed
   * @param rexList      list of decomposed RexNodes (except those with NOT)
   * @param notList      list of decomposed RexNodes that were prefixed NOT
   */
  public static void decomposeConjunction(
      RexNode rexPredicate,
      List<RexNode> rexList,
      List<RexNode> notList) {
    if (rexPredicate == null || rexPredicate.isAlwaysTrue()) {
      return;
    }
    switch (rexPredicate.getKind()) {
    case AND:
      for (RexNode operand : ((RexCall) rexPredicate).getOperands()) {
        decomposeConjunction(operand, rexList, notList);
      }
      break;
    case NOT:
      final RexNode e = ((RexCall) rexPredicate).getOperands().get(0);
      if (e.isAlwaysFalse()) {
        return;
      }
      notList.add(e);
      break;
    default:
      rexList.add(rexPredicate);
      break;
    }
  }

  /**
   * Decomposes a predicate into a list of expressions that are OR'ed
   * together.
   *
   * @param rexPredicate predicate to be analyzed
   * @param rexList      list of decomposed RexNodes
   */
  public static void decomposeDisjunction(
      RexNode rexPredicate,
      List<RexNode> rexList) {
    if (rexPredicate == null || rexPredicate.isAlwaysFalse()) {
      return;
    }
    if (rexPredicate.isA(SqlKind.OR)) {
      for (RexNode operand : ((RexCall) rexPredicate).getOperands()) {
        decomposeDisjunction(operand, rexList);
      }
    } else {
      rexList.add(rexPredicate);
    }
  }

  /**
   * Returns a condition decomposed by AND.
   *
   * <p>For example, {@code conjunctions(TRUE)} returns the empty list;
   * {@code conjunctions(FALSE)} returns list {@code {FALSE}}.</p>
   */
  public static List<RexNode> conjunctions(RexNode rexPredicate) {
    final List<RexNode> list = new ArrayList<>();
    decomposeConjunction(rexPredicate, list);
    return list;
  }

  /**
   * Returns a condition decomposed by OR.
   *
   * <p>For example, {@code disjunctions(FALSE)} returns the empty list.</p>
   */
  public static List<RexNode> disjunctions(RexNode rexPredicate) {
    final List<RexNode> list = new ArrayList<>();
    decomposeDisjunction(rexPredicate, list);
    return list;
  }

  /**
   * Ands two sets of join filters together, either of which can be null.
   *
   * @param rexBuilder rexBuilder to create AND expression
   * @param left       filter on the left that the right will be AND'd to
   * @param right      filter on the right
   * @return AND'd filter
   *
   * @see org.apache.calcite.rex.RexUtil#composeConjunction
   */
  public static RexNode andJoinFilters(
      RexBuilder rexBuilder,
      RexNode left,
      RexNode right) {
    // don't bother AND'ing in expressions that always evaluate to
    // true
    if ((left != null) && !left.isAlwaysTrue()) {
      if ((right != null) && !right.isAlwaysTrue()) {
        left =
            rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                left,
                right);
      }
    } else {
      left = right;
    }

    // Joins must have some filter
    if (left == null) {
      left = rexBuilder.makeLiteral(true);
    }
    return left;
  }

  /** Decomposes the WHERE clause of a view into predicates that constraint
   * a column to a particular value.
   *
   * <p>This method is key to the validation of a modifiable view. Columns that
   * are constrained to a single value can be omitted from the
   * SELECT clause of a modifiable view.
   *
   * @param projectMap Mapping from column ordinal to the expression that
   * populate that column, to be populated by this method
   * @param filters List of remaining filters, to be populated by this method
   * @param constraint Constraint to be analyzed
   */
  public static void inferViewPredicates(Map<Integer, RexNode> projectMap,
      List<RexNode> filters, RexNode constraint) {
    for (RexNode node : conjunctions(constraint)) {
      switch (node.getKind()) {
      case EQUALS:
        final List<RexNode> operands = ((RexCall) node).getOperands();
        RexNode o0 = operands.get(0);
        RexNode o1 = operands.get(1);
        if (o0 instanceof RexLiteral) {
          o0 = operands.get(1);
          o1 = operands.get(0);
        }
        if (o0.getKind() == SqlKind.CAST) {
          o0 = ((RexCall) o0).getOperands().get(0);
        }
        if (o0 instanceof RexInputRef && o1 instanceof RexLiteral) {
          final int index = ((RexInputRef) o0).getIndex();
          if (projectMap.get(index) == null) {
            projectMap.put(index, o1);
            continue;
          }
        }
      }
      filters.add(node);
    }
  }

  /**
   * Adjusts key values in a list by some fixed amount.
   *
   * @param keys       list of key values
   * @param adjustment the amount to adjust the key values by
   * @return modified list
   */
  public static List<Integer> adjustKeys(List<Integer> keys, int adjustment) {
    if (adjustment == 0) {
      return keys;
    }
    final List<Integer> newKeys = new ArrayList<>();
    for (int key : keys) {
      newKeys.add(key + adjustment);
    }
    return newKeys;
  }

  /**
   * Simplifies outer joins if filter above would reject nulls.
   *
   * @param joinRel Join
   * @param aboveFilters Filters from above
   * @param joinType Join type, can not be inner join
   */
  public static JoinRelType simplifyJoin(RelNode joinRel,
      ImmutableList<RexNode> aboveFilters,
      JoinRelType joinType) {
    final int nTotalFields = joinRel.getRowType().getFieldCount();
    final int nSysFields = 0;
    final int nFieldsLeft =
        joinRel.getInputs().get(0).getRowType().getFieldCount();
    final int nFieldsRight =
        joinRel.getInputs().get(1).getRowType().getFieldCount();
    assert nTotalFields == nSysFields + nFieldsLeft + nFieldsRight;

    // set the reference bitmaps for the left and right children
    ImmutableBitSet leftBitmap =
        ImmutableBitSet.range(nSysFields, nSysFields + nFieldsLeft);
    ImmutableBitSet rightBitmap =
        ImmutableBitSet.range(nSysFields + nFieldsLeft, nTotalFields);

    for (RexNode filter : aboveFilters) {
      if (joinType.generatesNullsOnLeft()
          && Strong.is(filter, leftBitmap)) {
        joinType = joinType.cancelNullsOnLeft();
      }
      if (joinType.generatesNullsOnRight()
          && Strong.is(filter, rightBitmap)) {
        joinType = joinType.cancelNullsOnRight();
      }
      if (joinType == JoinRelType.INNER) {
        break;
      }
    }
    return joinType;
  }

  /**
   * Classifies filters according to where they should be processed. They
   * either stay where they are, are pushed to the join (if they originated
   * from above the join), or are pushed to one of the children. Filters that
   * are pushed are added to list passed in as input parameters.
   *
   * @param joinRel      join node
   * @param filters      filters to be classified
   * @param joinType     join type
   * @param pushInto     whether filters can be pushed into the ON clause
   * @param pushLeft     true if filters can be pushed to the left
   * @param pushRight    true if filters can be pushed to the right
   * @param joinFilters  list of filters to push to the join
   * @param leftFilters  list of filters to push to the left child
   * @param rightFilters list of filters to push to the right child
   * @return whether at least one filter was pushed
   */
  public static boolean classifyFilters(
      RelNode joinRel,
      List<RexNode> filters,
      JoinRelType joinType,
      boolean pushInto,
      boolean pushLeft,
      boolean pushRight,
      List<RexNode> joinFilters,
      List<RexNode> leftFilters,
      List<RexNode> rightFilters) {
    RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
    List<RelDataTypeField> joinFields = joinRel.getRowType().getFieldList();
    final int nTotalFields = joinFields.size();
    final int nSysFields = 0; // joinRel.getSystemFieldList().size();
    final List<RelDataTypeField> leftFields =
        joinRel.getInputs().get(0).getRowType().getFieldList();
    final int nFieldsLeft = leftFields.size();
    final List<RelDataTypeField> rightFields =
        joinRel.getInputs().get(1).getRowType().getFieldList();
    final int nFieldsRight = rightFields.size();
    assert nTotalFields == (joinRel instanceof SemiJoin
        ? nSysFields + nFieldsLeft
        : nSysFields + nFieldsLeft + nFieldsRight);

    // set the reference bitmaps for the left and right children
    ImmutableBitSet leftBitmap =
        ImmutableBitSet.range(nSysFields, nSysFields + nFieldsLeft);
    ImmutableBitSet rightBitmap =
        ImmutableBitSet.range(nSysFields + nFieldsLeft, nTotalFields);

    final List<RexNode> filtersToRemove = Lists.newArrayList();
    for (RexNode filter : filters) {
      final InputFinder inputFinder = InputFinder.analyze(filter);
      final ImmutableBitSet inputBits = inputFinder.inputBitSet.build();

      // REVIEW - are there any expressions that need special handling
      // and therefore cannot be pushed?

      // filters can be pushed to the left child if the left child
      // does not generate NULLs and the only columns referenced in
      // the filter originate from the left child
      if (pushLeft && leftBitmap.contains(inputBits)) {
        // ignore filters that always evaluate to true
        if (!filter.isAlwaysTrue()) {
          // adjust the field references in the filter to reflect
          // that fields in the left now shift over by the number
          // of system fields
          final RexNode shiftedFilter =
              shiftFilter(
                  nSysFields,
                  nSysFields + nFieldsLeft,
                  -nSysFields,
                  rexBuilder,
                  joinFields,
                  nTotalFields,
                  leftFields,
                  filter);

          leftFilters.add(shiftedFilter);
        }
        filtersToRemove.add(filter);

        // filters can be pushed to the right child if the right child
        // does not generate NULLs and the only columns referenced in
        // the filter originate from the right child
      } else if (pushRight && rightBitmap.contains(inputBits)) {
        if (!filter.isAlwaysTrue()) {
          // adjust the field references in the filter to reflect
          // that fields in the right now shift over to the left;
          // since we never push filters to a NULL generating
          // child, the types of the source should match the dest
          // so we don't need to explicitly pass the destination
          // fields to RexInputConverter
          final RexNode shiftedFilter =
              shiftFilter(
                  nSysFields + nFieldsLeft,
                  nTotalFields,
                  -(nSysFields + nFieldsLeft),
                  rexBuilder,
                  joinFields,
                  nTotalFields,
                  rightFields,
                  filter);
          rightFilters.add(shiftedFilter);
        }
        filtersToRemove.add(filter);

      } else {
        // If the filter can't be pushed to either child and the join
        // is an inner join, push them to the join if they originated
        // from above the join
        if (joinType == JoinRelType.INNER && pushInto) {
          if (!joinFilters.contains(filter)) {
            joinFilters.add(filter);
          }
          filtersToRemove.add(filter);
        }
      }
    }

    // Remove filters after the loop, to prevent concurrent modification.
    if (!filtersToRemove.isEmpty()) {
      filters.removeAll(filtersToRemove);
    }

    // Did anything change?
    return !filtersToRemove.isEmpty();
  }

  private static RexNode shiftFilter(
      int start,
      int end,
      int offset,
      RexBuilder rexBuilder,
      List<RelDataTypeField> joinFields,
      int nTotalFields,
      List<RelDataTypeField> rightFields,
      RexNode filter) {
    int[] adjustments = new int[nTotalFields];
    for (int i = start; i < end; i++) {
      adjustments[i] = offset;
    }
    return filter.accept(
        new RexInputConverter(
            rexBuilder,
            joinFields,
            rightFields,
            adjustments));
  }

  /**
   * Splits a filter into two lists, depending on whether or not the filter
   * only references its child input
   *
   * @param childBitmap Fields in the child
   * @param predicate   filters that will be split
   * @param pushable    returns the list of filters that can be pushed to the
   *                    child input
   * @param notPushable returns the list of filters that cannot be pushed to
   *                    the child input
   */
  public static void splitFilters(
      ImmutableBitSet childBitmap,
      RexNode predicate,
      List<RexNode> pushable,
      List<RexNode> notPushable) {
    // for each filter, if the filter only references the child inputs,
    // then it can be pushed
    for (RexNode filter : conjunctions(predicate)) {
      ImmutableBitSet filterRefs = InputFinder.bits(filter);
      if (childBitmap.contains(filterRefs)) {
        pushable.add(filter);
      } else {
        notPushable.add(filter);
      }
    }
  }

  /**
   * Determines if a projection and its input reference identical input
   * references.
   *
   * @param project    projection being examined
   * @param checkNames if true, also compare that the names of the project
   *                   fields and its child fields
   * @return if checkNames is false, true is returned if the project and its
   * child reference the same input references, regardless of the names of the
   * project and child fields; if checkNames is true, then true is returned if
   * the input references are the same but the field names are different
   */
  public static boolean checkProjAndChildInputs(
      Project project,
      boolean checkNames) {
    int n = project.getProjects().size();
    RelDataType inputType = project.getInput().getRowType();
    if (inputType.getFieldList().size() != n) {
      return false;
    }
    List<RelDataTypeField> projFields = project.getRowType().getFieldList();
    List<RelDataTypeField> inputFields = inputType.getFieldList();
    boolean namesDifferent = false;
    for (int i = 0; i < n; ++i) {
      RexNode exp = project.getProjects().get(i);
      if (!(exp instanceof RexInputRef)) {
        return false;
      }
      RexInputRef fieldAccess = (RexInputRef) exp;
      if (i != fieldAccess.getIndex()) {
        // can't support reorder yet
        return false;
      }
      if (checkNames) {
        String inputFieldName = inputFields.get(i).getName();
        String projFieldName = projFields.get(i).getName();
        if (!projFieldName.equals(inputFieldName)) {
          namesDifferent = true;
        }
      }
    }

    // inputs are the same; return value depends on the checkNames
    // parameter
    return !checkNames || namesDifferent;
  }

  /**
   * Creates projection expressions reflecting the swapping of a join's input.
   *
   * @param newJoin   the RelNode corresponding to the join with its inputs
   *                  swapped
   * @param origJoin  original LogicalJoin
   * @param origOrder if true, create the projection expressions to reflect
   *                  the original (pre-swapped) join projection; otherwise,
   *                  create the projection to reflect the order of the swapped
   *                  projection
   * @return array of expression representing the swapped join inputs
   */
  public static List<RexNode> createSwappedJoinExprs(
      RelNode newJoin,
      Join origJoin,
      boolean origOrder) {
    final List<RelDataTypeField> newJoinFields =
        newJoin.getRowType().getFieldList();
    final RexBuilder rexBuilder = newJoin.getCluster().getRexBuilder();
    final List<RexNode> exps = new ArrayList<>();
    final int nFields =
        origOrder ? origJoin.getRight().getRowType().getFieldCount()
            : origJoin.getLeft().getRowType().getFieldCount();
    for (int i = 0; i < newJoinFields.size(); i++) {
      final int source = (i + nFields) % newJoinFields.size();
      RelDataTypeField field =
          origOrder ? newJoinFields.get(source) : newJoinFields.get(i);
      exps.add(rexBuilder.makeInputRef(field.getType(), source));
    }
    return exps;
  }

  /**
   * Converts a filter to the new filter that would result if the filter is
   * pushed past a LogicalProject that it currently is referencing.
   *
   * @param filter  the filter to be converted
   * @param projRel project rel underneath the filter
   * @return converted filter
   */
  public static RexNode pushFilterPastProject(
      RexNode filter,
      Project projRel) {
    // use RexPrograms to merge the filter and LogicalProject into a
    // single program so we can convert the LogicalFilter condition to
    // directly reference the LogicalProject's child
    RexBuilder rexBuilder = projRel.getCluster().getRexBuilder();
    RexProgram bottomProgram =
        RexProgram.create(
            projRel.getInput().getRowType(),
            projRel.getProjects(),
            null,
            projRel.getRowType(),
            rexBuilder);

    RexProgramBuilder topProgramBuilder =
        new RexProgramBuilder(
            projRel.getRowType(),
            rexBuilder);
    topProgramBuilder.addIdentity();
    topProgramBuilder.addCondition(filter);
    RexProgram topProgram = topProgramBuilder.getProgram();

    RexProgram mergedProgram =
        RexProgramBuilder.mergePrograms(
            topProgram,
            bottomProgram,
            rexBuilder);

    return mergedProgram.expandLocalRef(
        mergedProgram.getCondition());
  }

  /**
   * Creates a new {@link org.apache.calcite.rel.rules.MultiJoin} to reflect
   * projection references from a
   * {@link org.apache.calcite.rel.logical.LogicalProject} that is on top of the
   * {@link org.apache.calcite.rel.rules.MultiJoin}.
   *
   * @param multiJoin the original MultiJoin
   * @param project   the LogicalProject on top of the MultiJoin
   * @return the new MultiJoin
   */
  public static MultiJoin projectMultiJoin(
      MultiJoin multiJoin,
      LogicalProject project) {
    // Locate all input references in the projection expressions as well
    // the post-join filter.  Since the filter effectively sits in
    // between the LogicalProject and the MultiJoin, the projection needs
    // to include those filter references.
    ImmutableBitSet inputRefs =
        InputFinder.bits(project.getProjects(), multiJoin.getPostJoinFilter());

    // create new copies of the bitmaps
    List<RelNode> multiJoinInputs = multiJoin.getInputs();
    List<BitSet> newProjFields = Lists.newArrayList();
    for (RelNode multiJoinInput : multiJoinInputs) {
      newProjFields.add(
          new BitSet(multiJoinInput.getRowType().getFieldCount()));
    }

    // set the bits found in the expressions
    int currInput = -1;
    int startField = 0;
    int nFields = 0;
    for (int bit : inputRefs) {
      while (bit >= (startField + nFields)) {
        startField += nFields;
        currInput++;
        assert currInput < multiJoinInputs.size();
        nFields =
            multiJoinInputs.get(currInput).getRowType().getFieldCount();
      }
      newProjFields.get(currInput).set(bit - startField);
    }

    // create a new MultiJoin containing the new field bitmaps
    // for each input
    return new MultiJoin(
        multiJoin.getCluster(),
        multiJoin.getInputs(),
        multiJoin.getJoinFilter(),
        multiJoin.getRowType(),
        multiJoin.isFullOuterJoin(),
        multiJoin.getOuterJoinConditions(),
        multiJoin.getJoinTypes(),
        Lists.transform(newProjFields, ImmutableBitSet.FROM_BIT_SET),
        multiJoin.getJoinFieldRefCountsMap(),
        multiJoin.getPostJoinFilter());
  }

  public static <T extends RelNode> T addTrait(
      T rel, RelTrait trait) {
    //noinspection unchecked
    return (T) rel.copy(
        rel.getTraitSet().replace(trait),
        (List) rel.getInputs());
  }

  /**
   * Returns a shallow copy of a relational expression with a particular
   * input replaced.
   */
  public static RelNode replaceInput(
      RelNode parent, int ordinal, RelNode newInput) {
    final List<RelNode> inputs = new ArrayList<>(parent.getInputs());
    if (inputs.get(ordinal) == newInput) {
      return parent;
    }
    inputs.set(ordinal, newInput);
    return parent.copy(parent.getTraitSet(), inputs);
  }

  /**
   * Creates a {@link org.apache.calcite.rel.logical.LogicalProject} that
   * projects particular fields of its input, according to a mapping.
   */
  public static RelNode createProject(
      RelNode child,
      Mappings.TargetMapping mapping) {
    return createProject(child, Mappings.asList(mapping.inverse()));
  }

  /** Returns whether relational expression {@code target} occurs within a
   * relational expression {@code ancestor}. */
  public static boolean contains(RelNode ancestor, final RelNode target) {
    if (ancestor == target) {
      // Short-cut common case.
      return true;
    }
    try {
      new RelVisitor() {
        public void visit(RelNode node, int ordinal, RelNode parent) {
          if (node == target) {
            throw Util.FoundOne.NULL;
          }
          super.visit(node, ordinal, parent);
        }
      // CHECKSTYLE: IGNORE 1
      }.go(ancestor);
      return false;
    } catch (Util.FoundOne e) {
      return true;
    }
  }

  /** Within a relational expression {@code query}, replaces occurrences of
   * {@code find} with {@code replace}. */
  public static RelNode replace(RelNode query, RelNode find, RelNode replace) {
    if (find == replace) {
      // Short-cut common case.
      return query;
    }
    assert equalType("find", find, "replace", replace, true);
    if (query == find) {
      // Short-cut another common case.
      return replace;
    }
    return replaceRecurse(query, find, replace);
  }

  /** Helper for {@link #replace}. */
  private static RelNode replaceRecurse(
      RelNode query, RelNode find, RelNode replace) {
    if (query == find) {
      return replace;
    }
    final List<RelNode> inputs = query.getInputs();
    if (!inputs.isEmpty()) {
      final List<RelNode> newInputs = new ArrayList<>();
      for (RelNode input : inputs) {
        newInputs.add(replaceRecurse(input, find, replace));
      }
      if (!newInputs.equals(inputs)) {
        return query.copy(query.getTraitSet(), newInputs);
      }
    }
    return query;
  }

  /** Returns a simple
   * {@link org.apache.calcite.plan.RelOptTable.ToRelContext}. */
  public static RelOptTable.ToRelContext getContext(
      final RelOptCluster cluster) {
    return new RelOptTable.ToRelContext() {
      public RelOptCluster getCluster() {
        return cluster;
      }

      public RelNode expandView(
          RelDataType rowType,
          String queryString,
          List<String> schemaPath) {
        throw new UnsupportedOperationException();
      }
    };
  }

  /** Returns the number of {@link org.apache.calcite.rel.core.Join} nodes in a
   * tree. */
  public static int countJoins(RelNode rootRel) {
    /** Visitor that counts join nodes. */
    class JoinCounter extends RelVisitor {
      int joinCount;

      @Override public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof Join) {
          ++joinCount;
        }
        super.visit(node, ordinal, parent);
      }

      int run(RelNode node) {
        go(node);
        return joinCount;
      }
    }

    return new JoinCounter().run(rootRel);
  }

  /** Permutes a record type according to a mapping. */
  public static RelDataType permute(RelDataTypeFactory typeFactory,
      RelDataType rowType, Mapping mapping) {
    return typeFactory.createStructType(
        Mappings.apply3(mapping, rowType.getFieldList()));
  }

  /**
   * Creates a relational expression which projects a list of expressions.
   *
   * @param child         input relational expression
   * @param exprList      list of expressions for the input columns
   * @param fieldNameList aliases of the expressions, or null to generate
   */
  public static RelNode createProject(
      RelNode child,
      List<? extends RexNode> exprList,
      List<String> fieldNameList) {
    return createProject(child, exprList, fieldNameList, false);
  }

  /**
   * Creates a relational expression which projects a list of (expression, name)
   * pairs.
   *
   * @param child       input relational expression
   * @param projectList list of (expression, name) pairs
   * @param optimize    Whether to optimize
   */
  public static RelNode createProject(
      RelNode child,
      List<Pair<RexNode, String>> projectList,
      boolean optimize) {
    return createProject(child, Pair.left(projectList), Pair.right(projectList),
        optimize);
  }

  /**
   * Creates a relational expression that projects the given fields of the
   * input.
   *
   * <p>Optimizes if the fields are the identity projection.</p>
   *
   * @param child   Input relational expression
   * @param posList Source of each projected field
   * @return Relational expression that projects given fields
   */
  public static RelNode createProject(final RelNode child,
      final List<Integer> posList) {
    return createProject(
        RelFactories.DEFAULT_PROJECT_FACTORY, child, posList);
  }

  /**
   * Creates a relational expression which projects an array of expressions,
   * and optionally optimizes.
   *
   * <p>The result may not be a
   * {@link org.apache.calcite.rel.logical.LogicalProject}. If the
   * projection is trivial, <code>child</code> is returned directly; and future
   * versions may return other formulations of expressions, such as
   * {@link org.apache.calcite.rel.logical.LogicalCalc}.
   *
   * @param child      input relational expression
   * @param exprs      list of expressions for the input columns
   * @param fieldNames aliases of the expressions, or null to generate
   * @param optimize   Whether to return <code>child</code> unchanged if the
   *                   projections are trivial.
   */
  public static RelNode createProject(
      RelNode child,
      List<? extends RexNode> exprs,
      List<String> fieldNames,
      boolean optimize) {
    final RelOptCluster cluster = child.getCluster();
    final List<String> fieldNames2 =
        fieldNames == null
            ? null
            : SqlValidatorUtil.uniquify(fieldNames,
                SqlValidatorUtil.F_SUGGESTER);
    if (optimize
        && ProjectRemoveRule.isIdentity(exprs, child.getRowType())) {
      if (child instanceof Project && fieldNames != null) {
        final RelDataType rowType =
            RexUtil.createStructType(
                cluster.getTypeFactory(),
                exprs,
                fieldNames2);
        // Rename columns of child projection if desired field names are given.
        Project childProject = (Project) child;
        child = childProject.copy(childProject.getTraitSet(),
            childProject.getInput(), childProject.getProjects(), rowType);
      }
      return child;
    }
    return LogicalProject.create(child, exprs, fieldNames2);
  }

  /**
   * Returns a relational expression which has the same fields as the
   * underlying expression, but the fields have different names.
   *
   * @param rel        Relational expression
   * @param fieldNames Field names
   * @return Renamed relational expression
   */
  public static RelNode createRename(
      RelNode rel,
      List<String> fieldNames) {
    final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    assert fieldNames.size() == fields.size();
    final List<RexNode> refs =
        new AbstractList<RexNode>() {
          public int size() {
            return fields.size();
          }

          public RexNode get(int index) {
            return RexInputRef.of(index, fields);
          }
        };
    return createProject(rel, refs, fieldNames, true);
  }

  /**
   * Creates a relational expression which permutes the output fields of a
   * relational expression according to a permutation.
   *
   * <p>Optimizations:</p>
   *
   * <ul>
   * <li>If the relational expression is a
   * {@link org.apache.calcite.rel.logical.LogicalCalc} or
   * {@link org.apache.calcite.rel.logical.LogicalProject} that is already
   * acting as a permutation, combines the new permutation with the old;</li>
   *
   * <li>If the permutation is the identity, returns the original relational
   * expression.</li>
   * </ul>
   *
   * <p>If a permutation is combined with its inverse, these optimizations
   * would combine to remove them both.
   *
   * @param rel         Relational expression
   * @param permutation Permutation to apply to fields
   * @param fieldNames  Field names; if null, or if a particular entry is null,
   *                    the name of the permuted field is used
   * @return relational expression which permutes its input fields
   */
  public static RelNode permute(
      RelNode rel,
      Permutation permutation,
      List<String> fieldNames) {
    if (permutation.isIdentity()) {
      return rel;
    }
    if (rel instanceof LogicalCalc) {
      LogicalCalc calc = (LogicalCalc) rel;
      Permutation permutation1 = calc.getProgram().getPermutation();
      if (permutation1 != null) {
        Permutation permutation2 = permutation.product(permutation1);
        return permute(rel, permutation2, null);
      }
    }
    if (rel instanceof LogicalProject) {
      Permutation permutation1 = ((LogicalProject) rel).getPermutation();
      if (permutation1 != null) {
        Permutation permutation2 = permutation.product(permutation1);
        return permute(rel, permutation2, null);
      }
    }
    final List<RelDataType> outputTypeList = new ArrayList<>();
    final List<String> outputNameList = new ArrayList<>();
    final List<RexNode> exprList = new ArrayList<>();
    final List<RexLocalRef> projectRefList = new ArrayList<>();
    final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    final RelOptCluster cluster = rel.getCluster();
    for (int i = 0; i < permutation.getTargetCount(); i++) {
      int target = permutation.getTarget(i);
      final RelDataTypeField targetField = fields.get(target);
      outputTypeList.add(targetField.getType());
      outputNameList.add(
          ((fieldNames == null)
              || (fieldNames.size() <= i)
              || (fieldNames.get(i) == null)) ? targetField.getName()
              : fieldNames.get(i));
      exprList.add(
          cluster.getRexBuilder().makeInputRef(fields.get(i).getType(), i));
      final int source = permutation.getSource(i);
      projectRefList.add(
          new RexLocalRef(
              source,
              fields.get(source).getType()));
    }
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    final RexProgram program =
        new RexProgram(
            rel.getRowType(),
            exprList,
            projectRefList,
            null,
            typeFactory.createStructType(outputTypeList, outputNameList));
    return LogicalCalc.create(rel, program);
  }

  /**
   * Creates a relational expression that projects the given fields of the
   * input.
   *
   * <p>Optimizes if the fields are the identity projection.
   *
   * @param factory ProjectFactory
   * @param child Input relational expression
   * @param posList Source of each projected field
   * @return Relational expression that projects given fields
   */
  public static RelNode createProject(final RelFactories.ProjectFactory factory,
      final RelNode child, final List<Integer> posList) {
    RelDataType rowType = child.getRowType();
    if (Mappings.isIdentity(posList, rowType.getFieldCount())) {
      return child;
    }
    final List<String> fieldNames = rowType.getFieldNames();
    final RexBuilder rexBuilder = child.getCluster().getRexBuilder();
    return factory.createProject(child,
        new AbstractList<RexNode>() {
          public int size() {
            return posList.size();
          }

          public RexNode get(int index) {
            final int pos = posList.get(index);
            return rexBuilder.makeInputRef(child, pos);
          }
        },
        new AbstractList<String>() {
          public int size() {
            return posList.size();
          }

          public String get(int index) {
            final int pos = posList.get(index);
            return fieldNames.get(pos);
          }
        });
  }

  /**
   * Creates a relational expression which projects the output fields of a
   * relational expression according to a partial mapping.
   *
   * <p>A partial mapping is weaker than a permutation: every target has one
   * source, but a source may have 0, 1 or more than one targets. Usually the
   * result will have fewer fields than the source, unless some source fields
   * are projected multiple times.
   *
   * <p>This method could optimize the result as {@link #permute} does, but
   * does not at present.
   *
   * @param rel        Relational expression
   * @param mapping Mapping from source fields to target fields. The mapping
   * type must obey the constraints
   * {@link org.apache.calcite.util.mapping.MappingType#isMandatorySource()}
   * and
   * {@link org.apache.calcite.util.mapping.MappingType#isSingleSource()},
   * as does
   * {@link org.apache.calcite.util.mapping.MappingType#INVERSE_FUNCTION}.
   * @param fieldNames Field names; if null, or if a particular entry is null,
   *                   the name of the permuted field is used
   * @return relational expression which projects a subset of the input fields
   */
  public static RelNode projectMapping(
      RelNode rel,
      Mapping mapping,
      List<String> fieldNames,
      RelFactories.ProjectFactory projectFactory) {
    assert mapping.getMappingType().isSingleSource();
    assert mapping.getMappingType().isMandatorySource();
    if (mapping.isIdentity()) {
      return rel;
    }
    final List<String> outputNameList = Lists.newArrayList();
    final List<RexNode> exprList = Lists.newArrayList();
    final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    for (int i = 0; i < mapping.getTargetCount(); i++) {
      final int source = mapping.getSource(i);
      final RelDataTypeField sourceField = fields.get(source);
      outputNameList.add(
          ((fieldNames == null)
              || (fieldNames.size() <= i)
              || (fieldNames.get(i) == null)) ? sourceField.getName()
              : fieldNames.get(i));
      exprList.add(rexBuilder.makeInputRef(rel, source));
    }
    return projectFactory.createProject(rel, exprList, outputNameList);
  }

  /** Policies for handling two- and three-valued boolean logic. */
  public enum Logic {
    /** Three-valued boolean logic. */
    TRUE_FALSE_UNKNOWN,

    /** Nulls are not possible. */
    TRUE_FALSE,

    /** Two-valued logic where UNKNOWN is treated as FALSE.
     *
     * <p>"x IS TRUE" produces the same result, and "WHERE x", "JOIN ... ON x"
     * and "HAVING x" have the same effect. */
    UNKNOWN_AS_FALSE,

    /** Two-valued logic where UNKNOWN is treated as TRUE.
     *
     * <p>"x IS FALSE" produces the same result, as does "WHERE NOT x", etc.
     *
     * <p>In particular, this is the mode used by "WHERE k NOT IN q". If
     * "k IN q" produces TRUE or UNKNOWN, "NOT k IN q" produces FALSE or
     * UNKNOWN and the row is eliminated; if "k IN q" it returns FALSE, the
     * row is retained by the WHERE clause. */
     UNKNOWN_AS_TRUE,

    /** A semi-join will have been applied, so that only rows for which the
     * value is TRUE will have been returned. */
    TRUE;

    public Logic negate() {
      switch (this) {
      case UNKNOWN_AS_FALSE:
        return UNKNOWN_AS_TRUE;
      case UNKNOWN_AS_TRUE:
        return UNKNOWN_AS_FALSE;
      default:
        return this;
      }
    }
  }

  /**
   * Pushes down expressions in "equal" join condition.
   *
   * <p>For example, given
   * "emp JOIN dept ON emp.deptno + 1 = dept.deptno", adds a project above
   * "emp" that computes the expression
   * "emp.deptno + 1". The resulting join condition is a simple combination
   * of AND, equals, and input fields, plus the remaining non-equal conditions.
   *
   * @param originalJoin Join whose condition is to be pushed down
   */
  public static RelNode pushDownJoinConditions(Join originalJoin) {
    RexNode joinCond = originalJoin.getCondition();
    final JoinRelType joinType = originalJoin.getJoinType();
    RelNode leftRel = originalJoin.getLeft();
    RelNode rightRel = originalJoin.getRight();

    final List<RexNode> extraLeftExprs = new ArrayList<>();
    final List<RexNode> extraRightExprs = new ArrayList<>();
    final int leftCount = leftRel.getRowType().getFieldCount();
    final int rightCount = rightRel.getRowType().getFieldCount();

    if (!containsGet(joinCond)) {
      joinCond = pushDownEqualJoinConditions(
          joinCond, leftCount, rightCount, extraLeftExprs, extraRightExprs);
    }
    if (!extraLeftExprs.isEmpty()) {
      final List<RelDataTypeField> fields =
          leftRel.getRowType().getFieldList();
      leftRel = RelOptUtil.createProject(
          leftRel,
          new AbstractList<Pair<RexNode, String>>() {
            public int size() {
              return leftCount + extraLeftExprs.size();
            }

            public Pair<RexNode, String> get(int index) {
              if (index < leftCount) {
                RelDataTypeField field = fields.get(index);
                return Pair.<RexNode, String>of(
                    new RexInputRef(index, field.getType()),
                    field.getName());
              } else {
                return Pair.of(extraLeftExprs.get(index - leftCount), null);
              }
            }
          },
          true);
    }
    if (!extraRightExprs.isEmpty()) {
      final List<RelDataTypeField> fields =
          rightRel.getRowType().getFieldList();
      final int newLeftCount = leftCount + extraLeftExprs.size();
      rightRel = RelOptUtil.createProject(
          rightRel,
          new AbstractList<Pair<RexNode, String>>() {
            public int size() {
              return rightCount + extraRightExprs.size();
            }

            public Pair<RexNode, String> get(int index) {
              if (index < rightCount) {
                RelDataTypeField field = fields.get(index);
                return Pair.<RexNode, String>of(
                    new RexInputRef(index, field.getType()),
                    field.getName());
              } else {
                return Pair.of(
                    RexUtil.shift(
                        extraRightExprs.get(index - rightCount),
                        -newLeftCount),
                    null);
              }
            }
          },
          true);
    }

    RelNode join = originalJoin.copy(originalJoin.getTraitSet(),
        joinCond, leftRel, rightRel, joinType, originalJoin.isSemiJoinDone());

    if (!extraLeftExprs.isEmpty() || !extraRightExprs.isEmpty()) {
      Mappings.TargetMapping mapping =
          Mappings.createShiftMapping(
              leftCount + extraLeftExprs.size()
                  + rightCount + extraRightExprs.size(),
              0, 0, leftCount,
              leftCount, leftCount + extraLeftExprs.size(), rightCount);
      return RelOptUtil.createProject(join, mapping);
    }
    return join;
  }

  private static boolean containsGet(RexNode node) {
    try {
      node.accept(
          new RexVisitorImpl<Void>(true) {
            @Override public Void visitCall(RexCall call) {
              if (call.getOperator() == RexBuilder.GET_OPERATOR) {
                throw Util.FoundOne.NULL;
              }
              return super.visitCall(call);
            }
          });
      return false;
    } catch (Util.FoundOne e) {
      return true;
    }
  }

  /**
   * Pushes down parts of a join condition.
   *
   * <p>For example, given
   * "emp JOIN dept ON emp.deptno + 1 = dept.deptno", adds a project above
   * "emp" that computes the expression
   * "emp.deptno + 1". The resulting join condition is a simple combination
   * of AND, equals, and input fields.
   */
  private static RexNode pushDownEqualJoinConditions(
      RexNode node,
      int leftCount,
      int rightCount,
      List<RexNode> extraLeftExprs,
      List<RexNode> extraRightExprs) {
    switch (node.getKind()) {
    case AND:
    case EQUALS:
      final RexCall call = (RexCall) node;
      final List<RexNode> list = new ArrayList<>();
      List<RexNode> operands = Lists.newArrayList(call.getOperands());
      for (int i = 0; i < operands.size(); i++) {
        RexNode operand = operands.get(i);
        final int left2 = leftCount + extraLeftExprs.size();
        final int right2 = rightCount + extraRightExprs.size();
        final RexNode e =
            pushDownEqualJoinConditions(
                operand,
                leftCount,
                rightCount,
                extraLeftExprs,
                extraRightExprs);
        final List<RexNode> remainingOperands = Util.skip(operands, i + 1);
        final int left3 = leftCount + extraLeftExprs.size();
        fix(remainingOperands, left2, left3);
        fix(list, left2, left3);
        list.add(e);
      }
      if (!list.equals(call.getOperands())) {
        return call.clone(call.getType(), list);
      }
      return call;
    case OR:
    case INPUT_REF:
    case LITERAL:
      return node;
    default:
      final ImmutableBitSet bits = RelOptUtil.InputFinder.bits(node);
      final int mid = leftCount + extraLeftExprs.size();
      switch (Side.of(bits, mid)) {
      case LEFT:
        fix(extraRightExprs, mid, mid + 1);
        extraLeftExprs.add(node);
        return new RexInputRef(mid, node.getType());
      case RIGHT:
        final int index2 = mid + rightCount + extraRightExprs.size();
        extraRightExprs.add(node);
        return new RexInputRef(index2, node.getType());
      case BOTH:
      case EMPTY:
      default:
        return node;
      }
    }
  }

  private static void fix(List<RexNode> operands, int before, int after) {
    if (before == after) {
      return;
    }
    for (int i = 0; i < operands.size(); i++) {
      RexNode node = operands.get(i);
      operands.set(i, RexUtil.shift(node, before, after - before));
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Visitor that finds all variables used but not stopped in an expression. */
  private static class VariableSetVisitor extends RelVisitor {
    final Set<String> variables = new HashSet<>();

    // implement RelVisitor
    public void visit(
        RelNode p,
        int ordinal,
        RelNode parent) {
      super.visit(p, ordinal, parent);
      p.collectVariablesUsed(variables);

      // Important! Remove stopped variables AFTER we visit children
      // (which what super.visit() does)
      variables.removeAll(p.getVariablesStopped());
    }
  }

  /** Visitor that finds all variables used in an expression. */
  public static class VariableUsedVisitor extends RexShuttle {
    public final Set<String> variables = new LinkedHashSet<>();

    public RexNode visitCorrelVariable(RexCorrelVariable p) {
      variables.add(p.getName());
      return p;
    }
  }

  /** Shuttle that finds the set of inputs that are used. */
  public static class InputReferencedVisitor extends RexShuttle {
    public final SortedSet<Integer> inputPosReferenced = new TreeSet<>();

    public RexNode visitInputRef(RexInputRef inputRef) {
      inputPosReferenced.add(inputRef.getIndex());
      return inputRef;
    }
  }

  /** Converts types to descriptive strings. */
  public static class TypeDumper {
    private final String extraIndent = "  ";
    private String indent;
    private final PrintWriter pw;

    TypeDumper(PrintWriter pw) {
      this.pw = pw;
      this.indent = "";
    }

    void accept(RelDataType type) {
      if (type.isStruct()) {
        final List<RelDataTypeField> fields = type.getFieldList();

        // RECORD (
        //   I INTEGER NOT NULL,
        //   J VARCHAR(240))
        pw.println("RECORD (");
        String prevIndent = indent;
        this.indent = indent + extraIndent;
        acceptFields(fields);
        this.indent = prevIndent;
        pw.print(")");
        if (!type.isNullable()) {
          pw.print(" NOT NULL");
        }
      } else if (type instanceof MultisetSqlType) {
        // E.g. "INTEGER NOT NULL MULTISET NOT NULL"
        accept(type.getComponentType());
        pw.print(" MULTISET");
        if (!type.isNullable()) {
          pw.print(" NOT NULL");
        }
      } else {
        // E.g. "INTEGER" E.g. "VARCHAR(240) CHARACTER SET "ISO-8859-1"
        // COLLATE "ISO-8859-1$en_US$primary" NOT NULL"
        pw.print(type.getFullTypeString());
      }
    }

    private void acceptFields(final List<RelDataTypeField> fields) {
      for (int i = 0; i < fields.size(); i++) {
        RelDataTypeField field = fields.get(i);
        if (i > 0) {
          pw.println(",");
        }
        pw.print(indent);
        pw.print(field.getName());
        pw.print(" ");
        accept(field.getType());
      }
    }
  }

  /**
   * Visitor which builds a bitmap of the inputs used by an expression.
   */
  public static class InputFinder extends RexVisitorImpl<Void> {
    public final ImmutableBitSet.Builder inputBitSet;
    private final Set<RelDataTypeField> extraFields;

    public InputFinder() {
      this(null);
    }

    public InputFinder(Set<RelDataTypeField> extraFields) {
      super(true);
      this.inputBitSet = ImmutableBitSet.builder();
      this.extraFields = extraFields;
    }

    /** Returns an input finder that has analyzed a given expression. */
    public static InputFinder analyze(RexNode node) {
      final InputFinder inputFinder = new InputFinder();
      node.accept(inputFinder);
      return inputFinder;
    }

    /**
     * Returns a bit set describing the inputs used by an expression.
     */
    public static ImmutableBitSet bits(RexNode node) {
      return analyze(node).inputBitSet.build();
    }

    /**
     * Returns a bit set describing the inputs used by a collection of
     * project expressions and an optional condition.
     */
    public static ImmutableBitSet bits(List<RexNode> exprs, RexNode expr) {
      final InputFinder inputFinder = new InputFinder();
      RexUtil.apply(inputFinder, exprs, expr);
      return inputFinder.inputBitSet.build();
    }

    public Void visitInputRef(RexInputRef inputRef) {
      inputBitSet.set(inputRef.getIndex());
      return null;
    }

    @Override public Void visitCall(RexCall call) {
      if (call.getOperator() == RexBuilder.GET_OPERATOR) {
        RexLiteral literal = (RexLiteral) call.getOperands().get(1);
        extraFields.add(
            new RelDataTypeFieldImpl(
                (String) literal.getValue2(),
                -1,
                call.getType()));
      }
      return super.visitCall(call);
    }
  }

  /**
   * Walks an expression tree, converting the index of RexInputRefs based on
   * some adjustment factor.
   */
  public static class RexInputConverter extends RexShuttle {
    protected final RexBuilder rexBuilder;
    private final List<RelDataTypeField> srcFields;
    protected final List<RelDataTypeField> destFields;
    private final List<RelDataTypeField> leftDestFields;
    private final List<RelDataTypeField> rightDestFields;
    private final int nLeftDestFields;
    private final int[] adjustments;

    /**
     * @param rexBuilder      builder for creating new RexInputRefs
     * @param srcFields       fields where the RexInputRefs originated
     *                        from; if null, a new RexInputRef is always
     *                        created, referencing the input from destFields
     *                        corresponding to its current index value
     * @param destFields      fields that the new RexInputRefs will be
     *                        referencing; if null, use the type information
     *                        from the source field when creating the new
     *                        RexInputRef
     * @param leftDestFields  in the case where the destination is a join,
     *                        these are the fields from the left join input
     * @param rightDestFields in the case where the destination is a join,
     *                        these are the fields from the right join input
     * @param adjustments     the amount to adjust each field by
     */
    private RexInputConverter(
        RexBuilder rexBuilder,
        List<RelDataTypeField> srcFields,
        List<RelDataTypeField> destFields,
        List<RelDataTypeField> leftDestFields,
        List<RelDataTypeField> rightDestFields,
        int[] adjustments) {
      this.rexBuilder = rexBuilder;
      this.srcFields = srcFields;
      this.destFields = destFields;
      this.adjustments = adjustments;
      this.leftDestFields = leftDestFields;
      this.rightDestFields = rightDestFields;
      if (leftDestFields == null) {
        nLeftDestFields = 0;
      } else {
        assert destFields == null;
        nLeftDestFields = leftDestFields.size();
      }
    }

    public RexInputConverter(
        RexBuilder rexBuilder,
        List<RelDataTypeField> srcFields,
        List<RelDataTypeField> leftDestFields,
        List<RelDataTypeField> rightDestFields,
        int[] adjustments) {
      this(
          rexBuilder,
          srcFields,
          null,
          leftDestFields,
          rightDestFields,
          adjustments);
    }

    public RexInputConverter(
        RexBuilder rexBuilder,
        List<RelDataTypeField> srcFields,
        List<RelDataTypeField> destFields,
        int[] adjustments) {
      this(rexBuilder, srcFields, destFields, null, null, adjustments);
    }

    public RexInputConverter(
        RexBuilder rexBuilder,
        List<RelDataTypeField> srcFields,
        int[] adjustments) {
      this(rexBuilder, srcFields, null, null, null, adjustments);
    }

    public RexNode visitInputRef(RexInputRef var) {
      int srcIndex = var.getIndex();
      int destIndex = srcIndex + adjustments[srcIndex];

      RelDataType type;
      if (destFields != null) {
        type = destFields.get(destIndex).getType();
      } else if (leftDestFields != null) {
        if (destIndex < nLeftDestFields) {
          type = leftDestFields.get(destIndex).getType();
        } else {
          type =
              rightDestFields.get(destIndex - nLeftDestFields).getType();
        }
      } else {
        type = srcFields.get(srcIndex).getType();
      }
      if ((adjustments[srcIndex] != 0)
          || (srcFields == null)
          || (type != srcFields.get(srcIndex).getType())) {
        return rexBuilder.makeInputRef(type, destIndex);
      } else {
        return var;
      }
    }
  }

  /** What kind of sub-query. */
  public enum SubqueryType {
    EXISTS,
    IN,
    SCALAR
  }

  /**
   * Categorizes whether a bit set contains bits left and right of a
   * line.
   */
  enum Side {
    LEFT, RIGHT, BOTH, EMPTY;

    static Side of(ImmutableBitSet bitSet, int middle) {
      final int firstBit = bitSet.nextSetBit(0);
      if (firstBit < 0) {
        return EMPTY;
      }
      if (firstBit >= middle) {
        return RIGHT;
      }
      if (bitSet.nextSetBit(middle) < 0) {
        return LEFT;
      }
      return BOTH;
    }
  }
}

// End RelOptUtil.java
