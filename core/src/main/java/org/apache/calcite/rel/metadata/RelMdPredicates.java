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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

/**
 * Utility to infer Predicates that are applicable above a RelNode.
 *
 * <p>This is currently used by
 * {@link org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule} to
 * infer <em>Predicates</em> that can be inferred from one side of a Join
 * to the other.
 *
 * <p>The PullUp Strategy is sound but not complete. Here are some of the
 * limitations:
 * <ol>
 *
 * <li> For Aggregations we only PullUp predicates that only contain
 * Grouping Keys. This can be extended to infer predicates on Aggregation
 * expressions from  expressions on the aggregated columns. For e.g.
 * <pre>
 * select a, max(b) from R1 where b &gt; 7
 *   &rarr; max(b) &gt; 7 or max(b) is null
 * </pre>
 *
 * <li> For Projections we only look at columns that are projected without
 * any function applied. So:
 * <pre>
 * select a from R1 where a &gt; 7
 *   &rarr; "a &gt; 7" is pulled up from the Projection.
 * select a + 1 from R1 where a + 1 &gt; 7
 *   &rarr; "a + 1 gt; 7" is not pulled up
 * </pre>
 *
 * <li> There are several restrictions on Joins:
 *   <ul>
 *   <li> We only pullUp inferred predicates for now. Pulling up existing
 *   predicates causes an explosion of duplicates. The existing predicates
 *   are pushed back down as new predicates. Once we have rules to eliminate
 *   duplicate Filter conditions, we should pullUp all predicates.
 *
 *   <li> For Left Outer: we infer new predicates from the left and set them
 *   as applicable on the Right side. No predicates are pulledUp.
 *
 *   <li> Right Outer Joins are handled in an analogous manner.
 *
 *   <li> For Full Outer Joins no predicates are pulledUp or inferred.
 *   </ul>
 * </ol>
 */
public class RelMdPredicates {
  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
      .reflectiveSource(BuiltInMethod.PREDICATES.method, new RelMdPredicates());

  private static final List<RexNode> EMPTY_LIST = ImmutableList.of();

  // Catch-all rule when none of the others apply.
  public RelOptPredicateList getPredicates(RelNode rel) {
    return RelOptPredicateList.EMPTY;
  }

  /**
   * Infers predicates for a table scan.
   */
  public RelOptPredicateList getPredicates(TableScan table) {
    return RelOptPredicateList.EMPTY;
  }

  /**
   * Infers predicates for a project.
   *
   * <ol>
   * <li>create a mapping from input to projection. Map only positions that
   * directly reference an input column.
   * <li>Expressions that only contain above columns are retained in the
   * Project's pullExpressions list.
   * <li>For e.g. expression 'a + e = 9' below will not be pulled up because 'e'
   * is not in the projection list.
   *
   * <pre>
   * childPullUpExprs:      {a &gt; 7, b + c &lt; 10, a + e = 9}
   * projectionExprs:       {a, b, c, e / 2}
   * projectionPullupExprs: {a &gt; 7, b + c &lt; 10}
   * </pre>
   *
   * </ol>
   */
  public RelOptPredicateList getPredicates(Project project) {
    RelNode child = project.getInput();
    RelOptPredicateList childInfo =
        RelMetadataQuery.getPulledUpPredicates(child);

    List<RexNode> projectPullUpPredicates = new ArrayList<RexNode>();

    ImmutableBitSet.Builder columnsMappedBuilder = ImmutableBitSet.builder();
    Mapping m = Mappings.create(MappingType.PARTIAL_FUNCTION,
        child.getRowType().getFieldCount(),
        project.getRowType().getFieldCount());

    for (Ord<RexNode> o : Ord.zip(project.getProjects())) {
      if (o.e instanceof RexInputRef) {
        int sIdx = ((RexInputRef) o.e).getIndex();
        m.set(sIdx, o.i);
        columnsMappedBuilder.set(sIdx);
      }
    }

    // Go over childPullUpPredicates. If a predicate only contains columns in
    // 'columnsMapped' construct a new predicate based on mapping.
    final ImmutableBitSet columnsMapped = columnsMappedBuilder.build();
    for (RexNode r : childInfo.pulledUpPredicates) {
      ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(r);
      if (columnsMapped.contains(rCols)) {
        r = r.accept(new RexPermuteInputsShuttle(m, child));
        projectPullUpPredicates.add(r);
      }
    }
    return RelOptPredicateList.of(projectPullUpPredicates);
  }

  /**
   * Add the Filter condition to the pulledPredicates list from the child.
   */
  public RelOptPredicateList getPredicates(Filter filter) {
    RelNode child = filter.getInput();
    RelOptPredicateList childInfo =
        RelMetadataQuery.getPulledUpPredicates(child);

    return RelOptPredicateList.of(
        Iterables.concat(childInfo.pulledUpPredicates,
            RelOptUtil.conjunctions(filter.getCondition())));
  }

  /** Infers predicates for a {@link org.apache.calcite.rel.core.SemiJoin}. */
  public RelOptPredicateList getPredicates(SemiJoin semiJoin) {
    // Workaround, pending [CALCITE-390] "Transitive inference (RelMdPredicate)
    // doesn't handle semi-join"
    return RelOptPredicateList.EMPTY;
  }

  /** Infers predicates for a {@link org.apache.calcite.rel.core.Join}. */
  public RelOptPredicateList getPredicates(Join join) {
    RexBuilder rB = join.getCluster().getRexBuilder();
    RelNode left = join.getInput(0);
    RelNode right = join.getInput(1);

    RelOptPredicateList leftInfo =
        RelMetadataQuery.getPulledUpPredicates(left);
    RelOptPredicateList rightInfo =
        RelMetadataQuery.getPulledUpPredicates(right);

    JoinConditionBasedPredicateInference jI =
        new JoinConditionBasedPredicateInference(join,
            RexUtil.composeConjunction(rB, leftInfo.pulledUpPredicates, false),
            RexUtil.composeConjunction(rB, rightInfo.pulledUpPredicates,
                false));

    return jI.inferPredicates(false);
  }

  /**
   * Infers predicates for an Aggregate.
   *
   * <p>Pulls up predicates that only contains references to columns in the
   * GroupSet. For e.g.
   *
   * <pre>
   * childPullUpExprs : { a &gt; 7, b + c &lt; 10, a + e = 9}
   * groupSet         : { a, b}
   * pulledUpExprs    : { a &gt; 7}
   * </pre>
   */
  public RelOptPredicateList getPredicates(Aggregate agg) {
    RelNode child = agg.getInput();
    RelOptPredicateList childInfo =
        RelMetadataQuery.getPulledUpPredicates(child);

    List<RexNode> aggPullUpPredicates = new ArrayList<RexNode>();

    ImmutableBitSet groupKeys = agg.getGroupSet();
    Mapping m = Mappings.create(MappingType.PARTIAL_FUNCTION,
        child.getRowType().getFieldCount(), agg.getRowType().getFieldCount());

    int i = 0;
    for (int j : groupKeys) {
      m.set(j, i++);
    }

    for (RexNode r : childInfo.pulledUpPredicates) {
      ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(r);
      if (groupKeys.contains(rCols)) {
        r = r.accept(new RexPermuteInputsShuttle(m, child));
        aggPullUpPredicates.add(r);
      }
    }
    return RelOptPredicateList.of(aggPullUpPredicates);
  }

  /**
   * Infers predicates for a Union.
   *
   * <p>The pulled up expression is a disjunction of its children's predicates.
   */
  public RelOptPredicateList getPredicates(Union union) {
    RexBuilder rB = union.getCluster().getRexBuilder();
    List<RexNode> orList = Lists.newArrayList();
    for (RelNode input : union.getInputs()) {
      RelOptPredicateList info = RelMetadataQuery.getPulledUpPredicates(input);
      if (info.pulledUpPredicates.isEmpty()) {
        return RelOptPredicateList.EMPTY;
      }
      RelOptUtil.decomposeDisjunction(
          RexUtil.composeConjunction(rB, info.pulledUpPredicates, false),
          orList);
    }

    if (orList.isEmpty()) {
      return RelOptPredicateList.EMPTY;
    }
    return RelOptPredicateList.of(
        RelOptUtil.conjunctions(RexUtil.composeDisjunction(rB, orList, false)));
  }

  /**
   * Infers predicates for a Sort.
   */
  public RelOptPredicateList getPredicates(Sort sort) {
    RelNode child = sort.getInput();
    return RelMetadataQuery.getPulledUpPredicates(child);
  }

  /**
   * Infers predicates for an Exchange.
   */
  public RelOptPredicateList getPredicates(Exchange exchange) {
    RelNode child = exchange.getInput();
    return RelMetadataQuery.getPulledUpPredicates(child);
  }

  /**
   * Utility to infer predicates from one side of the join that apply on the
   * other side.
   *
   * <p>Contract is:<ul>
   *
   * <li>initialize with a {@link org.apache.calcite.rel.core.Join} and
   * optional predicates applicable on its left and right subtrees.
   *
   * <li>you can
   * then ask it for equivalentPredicate(s) given a predicate.
   *
   * </ul>
   *
   * <p>So for:
   * <ol>
   * <li>'<code>R1(x) join R2(y) on x = y</code>' a call for
   * equivalentPredicates on '<code>x > 7</code>' will return '
   * <code>[y > 7]</code>'
   * <li>'<code>R1(x) join R2(y) on x = y join R3(z) on y = z</code>' a call for
   * equivalentPredicates on the second join '<code>x > 7</code>' will return '
   * <code>[y > 7, z > 7]</code>'
   * </ol>
   */
  static class JoinConditionBasedPredicateInference {
    final Join joinRel;
    final int nSysFields;
    final int nFieldsLeft;
    final int nFieldsRight;
    final ImmutableBitSet leftFieldsBitSet;
    final ImmutableBitSet rightFieldsBitSet;
    final ImmutableBitSet allFieldsBitSet;
    SortedMap<Integer, BitSet> equivalence;
    final Map<String, ImmutableBitSet> exprFields;
    final Set<String> allExprsDigests;
    final Set<String> equalityPredicates;
    final RexNode leftChildPredicates;
    final RexNode rightChildPredicates;

    public JoinConditionBasedPredicateInference(Join joinRel,
        RexNode lPreds, RexNode rPreds) {
      super();
      this.joinRel = joinRel;
      nFieldsLeft = joinRel.getLeft().getRowType().getFieldList().size();
      nFieldsRight = joinRel.getRight().getRowType().getFieldList().size();
      nSysFields = joinRel.getSystemFieldList().size();
      leftFieldsBitSet = ImmutableBitSet.range(nSysFields,
          nSysFields + nFieldsLeft);
      rightFieldsBitSet = ImmutableBitSet.range(nSysFields + nFieldsLeft,
          nSysFields + nFieldsLeft + nFieldsRight);
      allFieldsBitSet = ImmutableBitSet.range(0,
          nSysFields + nFieldsLeft + nFieldsRight);

      exprFields = Maps.newHashMap();
      allExprsDigests = new HashSet<String>();

      if (lPreds == null) {
        leftChildPredicates = null;
      } else {
        Mappings.TargetMapping leftMapping = Mappings.createShiftMapping(
            nSysFields + nFieldsLeft, nSysFields, 0, nFieldsLeft);
        leftChildPredicates = lPreds.accept(
            new RexPermuteInputsShuttle(leftMapping, joinRel.getInput(0)));

        for (RexNode r : RelOptUtil.conjunctions(leftChildPredicates)) {
          exprFields.put(r.toString(), RelOptUtil.InputFinder.bits(r));
          allExprsDigests.add(r.toString());
        }
      }
      if (rPreds == null) {
        rightChildPredicates = null;
      } else {
        Mappings.TargetMapping rightMapping = Mappings.createShiftMapping(
            nSysFields + nFieldsLeft + nFieldsRight,
            nSysFields + nFieldsLeft, 0, nFieldsRight);
        rightChildPredicates = rPreds.accept(
            new RexPermuteInputsShuttle(rightMapping, joinRel.getInput(1)));

        for (RexNode r : RelOptUtil.conjunctions(rightChildPredicates)) {
          exprFields.put(r.toString(), RelOptUtil.InputFinder.bits(r));
          allExprsDigests.add(r.toString());
        }
      }

      equivalence = Maps.newTreeMap();
      equalityPredicates = new HashSet<String>();
      for (int i = 0; i < nSysFields + nFieldsLeft + nFieldsRight; i++) {
        equivalence.put(i, BitSets.of(i));
      }

      // Only process equivalences found in the join conditions. Processing
      // Equivalences from the left or right side infer predicates that are
      // already present in the Tree below the join.
      RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
      List<RexNode> exprs =
          RelOptUtil.conjunctions(
              compose(rexBuilder, ImmutableList.of(joinRel.getCondition())));

      final EquivalenceFinder eF = new EquivalenceFinder();
      new ArrayList<Void>(Lists.transform(exprs, new Function<RexNode, Void>() {
        public Void apply(RexNode input) {
          return input.accept(eF);
        }
      }));

      equivalence = BitSets.closure(equivalence);
    }

    /**
     * The PullUp Strategy is sound but not complete.
     * <ol>
     * <li>We only pullUp inferred predicates for now. Pulling up existing
     * predicates causes an explosion of duplicates. The existing predicates are
     * pushed back down as new predicates. Once we have rules to eliminate
     * duplicate Filter conditions, we should pullUp all predicates.
     * <li>For Left Outer: we infer new predicates from the left and set them as
     * applicable on the Right side. No predicates are pulledUp.
     * <li>Right Outer Joins are handled in an analogous manner.
     * <li>For Full Outer Joins no predicates are pulledUp or inferred.
     * </ol>
     */
    public RelOptPredicateList inferPredicates(
        boolean includeEqualityInference) {
      List<RexNode> inferredPredicates = new ArrayList<RexNode>();
      Set<String> allExprsDigests = new HashSet<String>(this.allExprsDigests);
      final JoinRelType joinType = joinRel.getJoinType();
      switch (joinType) {
      case INNER:
      case LEFT:
        infer(leftChildPredicates, allExprsDigests, inferredPredicates,
            includeEqualityInference,
            joinType == JoinRelType.LEFT ? rightFieldsBitSet
                : allFieldsBitSet);
        break;
      }
      switch (joinType) {
      case INNER:
      case RIGHT:
        infer(rightChildPredicates, allExprsDigests, inferredPredicates,
            includeEqualityInference,
            joinType == JoinRelType.RIGHT ? leftFieldsBitSet
                : allFieldsBitSet);
        break;
      }

      Mappings.TargetMapping rightMapping = Mappings.createShiftMapping(
          nSysFields + nFieldsLeft + nFieldsRight,
          0, nSysFields + nFieldsLeft, nFieldsRight);
      final RexPermuteInputsShuttle rightPermute =
          new RexPermuteInputsShuttle(rightMapping, joinRel);
      Mappings.TargetMapping leftMapping = Mappings.createShiftMapping(
          nSysFields + nFieldsLeft, 0, nSysFields, nFieldsLeft);
      final RexPermuteInputsShuttle leftPermute =
          new RexPermuteInputsShuttle(leftMapping, joinRel);
      List<RexNode> leftInferredPredicates = new ArrayList<RexNode>();
      List<RexNode> rightInferredPredicates = new ArrayList<RexNode>();

      for (RexNode iP : inferredPredicates) {
        ImmutableBitSet iPBitSet = RelOptUtil.InputFinder.bits(iP);
        if (leftFieldsBitSet.contains(iPBitSet)) {
          leftInferredPredicates.add(iP.accept(leftPermute));
        } else if (rightFieldsBitSet.contains(iPBitSet)) {
          rightInferredPredicates.add(iP.accept(rightPermute));
        }
      }

      switch (joinType) {
      case INNER:
        return RelOptPredicateList.of(
            Iterables.concat(RelOptUtil.conjunctions(leftChildPredicates),
                RelOptUtil.conjunctions(rightChildPredicates),
                RelOptUtil.conjunctions(joinRel.getCondition()),
                inferredPredicates),
            leftInferredPredicates, rightInferredPredicates);
      case LEFT:
        return RelOptPredicateList.of(
            RelOptUtil.conjunctions(leftChildPredicates),
            leftInferredPredicates, rightInferredPredicates);
      case RIGHT:
        return RelOptPredicateList.of(
            RelOptUtil.conjunctions(rightChildPredicates),
            inferredPredicates, EMPTY_LIST);
      default:
        assert inferredPredicates.size() == 0;
        return RelOptPredicateList.EMPTY;
      }
    }

    public RexNode left() {
      return leftChildPredicates;
    }

    public RexNode right() {
      return rightChildPredicates;
    }

    private void infer(RexNode predicates, Set<String> allExprsDigests,
        List<RexNode> inferedPredicates, boolean includeEqualityInference,
        ImmutableBitSet inferringFields) {
      for (RexNode r : RelOptUtil.conjunctions(predicates)) {
        if (!includeEqualityInference
            && equalityPredicates.contains(r.toString())) {
          continue;
        }
        for (Mapping m : mappings(r)) {
          RexNode tr = r.accept(
              new RexPermuteInputsShuttle(m, joinRel.getInput(0),
                  joinRel.getInput(1)));
          if (inferringFields.contains(RelOptUtil.InputFinder.bits(tr))
              && !allExprsDigests.contains(tr.toString())
              && !isAlwaysTrue(tr)) {
            inferedPredicates.add(tr);
            allExprsDigests.add(tr.toString());
          }
        }
      }
    }

    Iterable<Mapping> mappings(final RexNode predicate) {
      return new Iterable<Mapping>() {
        public Iterator<Mapping> iterator() {
          ImmutableBitSet fields = exprFields.get(predicate.toString());
          if (fields.cardinality() == 0) {
            return Iterators.emptyIterator();
          }
          return new ExprsItr(fields);
        }
      };
    }

    private void equivalent(int p1, int p2) {
      BitSet b = equivalence.get(p1);
      b.set(p2);

      b = equivalence.get(p2);
      b.set(p1);
    }

    RexNode compose(RexBuilder rexBuilder, Iterable<RexNode> exprs) {
      exprs = Linq4j.asEnumerable(exprs).where(new Predicate1<RexNode>() {
        public boolean apply(RexNode expr) {
          return expr != null;
        }
      });
      return RexUtil.composeConjunction(rexBuilder, exprs, false);
    }

    /**
     * Find expressions of the form 'col_x = col_y'.
     */
    class EquivalenceFinder extends RexVisitorImpl<Void> {
      protected EquivalenceFinder() {
        super(true);
      }

      @Override public Void visitCall(RexCall call) {
        if (call.getOperator().getKind() == SqlKind.EQUALS) {
          int lPos = pos(call.getOperands().get(0));
          int rPos = pos(call.getOperands().get(1));
          if (lPos != -1 && rPos != -1) {
            JoinConditionBasedPredicateInference.this.equivalent(lPos, rPos);
            JoinConditionBasedPredicateInference.this.equalityPredicates
                .add(call.toString());
          }
        }
        return null;
      }
    }

    /**
     * Given an expression returns all the possible substitutions.
     *
     * <p>For example, for an expression 'a + b + c' and the following
     * equivalences: <pre>
     * a : {a, b}
     * b : {a, b}
     * c : {c, e}
     * </pre>
     *
     * <p>The following Mappings will be returned:
     * <pre>
     * {a->a, b->a, c->c}
     * {a->a, b->a, c->e}
     * {a->a, b->b, c->c}
     * {a->a, b->b, c->e}
     * {a->b, b->a, c->c}
     * {a->b, b->a, c->e}
     * {a->b, b->b, c->c}
     * {a->b, b->b, c->e}
     * </pre>
     *
     * <p>which imply the following inferences:
     * <pre>
     * a + a + c
     * a + a + e
     * a + b + c
     * a + b + e
     * b + a + c
     * b + a + e
     * b + b + c
     * b + b + e
     * </pre>
     */
    class ExprsItr implements Iterator<Mapping> {
      final int[] columns;
      final BitSet[] columnSets;
      final int[] iterationIdx;
      Mapping nextMapping;
      boolean firstCall;

      ExprsItr(ImmutableBitSet fields) {
        nextMapping = null;
        columns = new int[fields.cardinality()];
        columnSets = new BitSet[fields.cardinality()];
        iterationIdx = new int[fields.cardinality()];
        for (int j = 0, i = fields.nextSetBit(0); i >= 0; i = fields
            .nextSetBit(i + 1), j++) {
          columns[j] = i;
          columnSets[j] = equivalence.get(i);
          iterationIdx[j] = 0;
        }
        firstCall = true;
      }

      public boolean hasNext() {
        if (firstCall) {
          initializeMapping();
          firstCall = false;
        } else {
          computeNextMapping(iterationIdx.length - 1);
        }
        return nextMapping != null;
      }

      public Mapping next() {
        return nextMapping;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }

      private void computeNextMapping(int level) {
        int t = columnSets[level].nextSetBit(iterationIdx[level]);
        if (t < 0) {
          if (level == 0) {
            nextMapping = null;
          } else {
            iterationIdx[level] = 0;
            computeNextMapping(level - 1);
          }
        } else {
          nextMapping.set(columns[level], t);
          iterationIdx[level] = t + 1;
        }
      }

      private void initializeMapping() {
        nextMapping = Mappings.create(MappingType.PARTIAL_FUNCTION,
            nSysFields + nFieldsLeft + nFieldsRight,
            nSysFields + nFieldsLeft + nFieldsRight);
        for (int i = 0; i < columnSets.length; i++) {
          BitSet c = columnSets[i];
          int t = c.nextSetBit(iterationIdx[i]);
          if (t < 0) {
            nextMapping = null;
            return;
          }
          nextMapping.set(columns[i], t);
          iterationIdx[i] = t + 1;
        }
      }
    }

    private int pos(RexNode expr) {
      if (expr instanceof RexInputRef) {
        return ((RexInputRef) expr).getIndex();
      }
      return -1;
    }

    private boolean isAlwaysTrue(RexNode predicate) {
      if (predicate instanceof RexCall) {
        RexCall c = (RexCall) predicate;
        if (c.getOperator().getKind() == SqlKind.EQUALS) {
          int lPos = pos(c.getOperands().get(0));
          int rPos = pos(c.getOperands().get(1));
          return lPos != -1 && lPos == rPos;
        }
      }
      return predicate.isAlwaysTrue();
    }
  }
}

// End RelMdPredicates.java
