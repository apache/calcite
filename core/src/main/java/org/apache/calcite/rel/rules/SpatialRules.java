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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.HilbertCurve2D;
import org.apache.calcite.runtime.SpaceFillingCurve2D;
import org.apache.calcite.runtime.SpatialTypeFunctions;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.apache.calcite.rex.RexLiteral.value;

import static java.util.Objects.requireNonNull;

/**
 * Collection of planner rules that convert
 * calls to spatial functions into more efficient expressions.
 *
 * <p>The rules allow Calcite to use spatial indexes. For example the following
 * query:
 *
 * <blockquote>SELECT ...
 * FROM Restaurants AS r
 * WHERE ST_DWithin(ST_Point(10, 20), ST_Point(r.longitude, r.latitude), 5)
 * </blockquote>
 *
 * <p>is rewritten to
 *
 * <blockquote>SELECT ...
 * FROM Restaurants AS r
 * WHERE (r.h BETWEEN 100 AND 150
 *        OR r.h BETWEEN 170 AND 185)
 * AND ST_DWithin(ST_Point(10, 20), ST_Point(r.longitude, r.latitude), 5)
 * </blockquote>
 *
 * <p>if there is the constraint
 *
 * <blockquote>CHECK (h = Hilbert(8, r.longitude, r.latitude))</blockquote>
 *
 * <p>If the {@code Restaurants} table is sorted on {@code h} then the latter
 * query can be answered using two limited range-scans, and so is much more
 * efficient.
 *
 * <p>Note that the original predicate
 * {@code ST_DWithin(ST_Point(10, 20), ST_Point(r.longitude, r.latitude), 5)}
 * is still present, but is evaluated after the approximate predicate has
 * eliminated many potential matches.
 */
@Value.Enclosing
public abstract class SpatialRules {

  private SpatialRules() {}

  private static final RexUtil.RexFinder DWITHIN_FINDER =
      RexUtil.find(EnumSet.of(SqlKind.ST_DWITHIN, SqlKind.ST_CONTAINS));

  private static final RexUtil.RexFinder HILBERT_FINDER =
      RexUtil.find(SqlKind.HILBERT);

  public static final RelOptRule INSTANCE =
      FilterHilbertRule.Config.DEFAULT.toRule();

  /** Returns a geometry if an expression is constant, null otherwise. */
  private static @Nullable Geometry constantGeom(RexNode e) {
    switch (e.getKind()) {
    case CAST:
      return constantGeom(((RexCall) e).getOperands().get(0));
    case LITERAL:
      return (Geometry) ((RexLiteral) e).getValue();
    default:
      return null;
    }
  }

  /** Rule that converts ST_DWithin in a Filter condition into a predicate on
   * a Hilbert curve. */
  @SuppressWarnings("WeakerAccess")
  public static class FilterHilbertRule
      extends RelRule<FilterHilbertRule.Config> {
    protected FilterHilbertRule(Config config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      final List<RexNode> conjunctions = new ArrayList<>();
      RelOptUtil.decomposeConjunction(filter.getCondition(), conjunctions);

      // Match a predicate
      //   r.hilbert = hilbert(r.longitude, r.latitude)
      // to one of the conjunctions
      //   ST_DWithin(ST_Point(x, y), ST_Point(r.longitude, r.latitude), d)
      // and if it matches add a new conjunction before it,
      //   r.hilbert between h1 and h2
      //   or r.hilbert between h3 and h4
      // where {[h1, h2], [h3, h4]} are the ranges of the Hilbert curve
      // intersecting the square
      //   (r.longitude - d, r.latitude - d, r.longitude + d, r.latitude + d)
      final RelOptPredicateList predicates =
          call.getMetadataQuery().getAllPredicates(filter.getInput());
      if (predicates == null) {
        return;
      }
      int changeCount = 0;
      for (RexNode predicate : predicates.pulledUpPredicates) {
        final RelBuilder builder = call.builder();
        if (predicate.getKind() == SqlKind.EQUALS) {
          final RexCall eqCall = (RexCall) predicate;
          if (eqCall.operands.get(0) instanceof RexInputRef
              && eqCall.operands.get(1).getKind() == SqlKind.HILBERT) {
            final RexInputRef ref  = (RexInputRef) eqCall.operands.get(0);
            final RexCall hilbert = (RexCall) eqCall.operands.get(1);
            final RexUtil.RexFinder finder = RexUtil.find(ref);
            if (finder.anyContain(conjunctions)) {
              // If the condition already contains "ref", it is probable that
              // this rule has already fired once.
              continue;
            }
            for (int i = 0; i < conjunctions.size();) {
              final List<RexNode> replacements =
                  replaceSpatial(conjunctions.get(i), builder, ref, hilbert);
              if (replacements != null) {
                conjunctions.remove(i);
                conjunctions.addAll(i, replacements);
                i += replacements.size();
                ++changeCount;
              } else {
                ++i;
              }
            }
          }
        }
        if (changeCount > 0) {
          call.transformTo(
              builder.push(filter.getInput())
                  .filter(conjunctions)
                  .build());
          return; // we found one useful constraint; don't look for more
        }
      }
    }

    /** Rewrites a spatial predicate to a predicate on a Hilbert curve.
     *
     * <p>Returns null if the predicate cannot be rewritten;
     * a 1-element list (new) if the predicate can be fully rewritten;
     * returns a 2-element list (new, original) if the new predicate allows
     * some false positives.
     *
     * @param conjunction Original predicate
     * @param builder Builder
     * @param ref Reference to Hilbert column
     * @param hilbert Function call that populates Hilbert column
     *
     * @return List containing rewritten predicate and original, or null
     */
    static @Nullable List<RexNode> replaceSpatial(RexNode conjunction, RelBuilder builder,
        RexInputRef ref, RexCall hilbert) {
      final RexNode op0;
      final RexNode op1;
      final Geometry g0;
      switch (conjunction.getKind()) {
      case ST_DWITHIN:
        final RexCall within = (RexCall) conjunction;
        op0 = within.operands.get(0);
        g0 = constantGeom(op0);
        op1 = within.operands.get(1);
        final Geometry g1 = constantGeom(op1);
        if (RexUtil.isLiteral(within.operands.get(2), true)) {
          final Number distance = requireNonNull(
              (Number) value(within.operands.get(2)),
              () -> "distance for " + within);
          switch (Double.compare(distance.doubleValue(), 0D)) {
          case -1: // negative distance
            return ImmutableList.of(builder.getRexBuilder().makeLiteral(false));

          case 0: // zero distance
            // Change "ST_DWithin(g, p, 0)" to "g = p"
            conjunction = builder.equals(op0, op1);
            // fall through

          case 1:
            if (g0 != null
                && op1.getKind() == SqlKind.ST_POINT
                && ((RexCall) op1).operands.equals(hilbert.operands)) {
              // Add the new predicate before the existing predicate
              // because it is cheaper to execute (albeit less selective).
              return ImmutableList.of(
                  hilbertPredicate(builder.getRexBuilder(), ref, g0, distance),
                  conjunction);
            } else if (g1 != null && op0.getKind() == SqlKind.ST_POINT
                && ((RexCall) op0).operands.equals(hilbert.operands)) {
              // Add the new predicate before the existing predicate
              // because it is cheaper to execute (albeit less selective).
              return ImmutableList.of(
                  hilbertPredicate(builder.getRexBuilder(), ref, g1, distance),
                  conjunction);
            }
            return null; // cannot rewrite

          default:
            throw new AssertionError("invalid sign: " + distance);
          }
        }
        return null; // cannot rewrite

      case ST_CONTAINS:
        final RexCall contains = (RexCall) conjunction;
        op0 = contains.operands.get(0);
        g0 = constantGeom(op0);
        op1 = contains.operands.get(1);
        if (g0 != null
            && op1.getKind() == SqlKind.ST_POINT
            && ((RexCall) op1).operands.equals(hilbert.operands)) {
          // Add the new predicate before the existing predicate
          // because it is cheaper to execute (albeit less selective).
          return ImmutableList.of(
              hilbertPredicate(builder.getRexBuilder(), ref, g0),
              conjunction);
        }
        return null; // cannot rewrite

      default:
        return null; // cannot rewrite
      }
    }

    /** Creates a predicate on the column that contains the index on the Hilbert
     * curve.
     *
     * <p>The predicate is a safe approximation. That is, it may allow some
     * points that are not within the distance, but will never disallow a point
     * that is within the distance.
     *
     * <p>Returns FALSE if the distance is negative (the ST_DWithin function
     * would always return FALSE) and returns an {@code =} predicate if distance
     * is 0. But usually returns a list of ranges,
     * {@code ref BETWEEN c1 AND c2 OR ref BETWEEN c3 AND c4}. */
    private static RexNode hilbertPredicate(RexBuilder rexBuilder,
        RexInputRef ref, Geometry g, Number distance) {
      if (distance.doubleValue() == 0D && g instanceof Point) {
        final Point p = (Point) g;
        final HilbertCurve2D hilbert = new HilbertCurve2D(8);
        final long index = hilbert.toIndex(p.getX(), p.getY());
        return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref,
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(index)));
      }
      final Geometry g2 =
          SpatialTypeFunctions.ST_Buffer(g, distance.doubleValue());
      return hilbertPredicate(rexBuilder, ref, g2);
    }

    private static RexNode hilbertPredicate(RexBuilder rexBuilder,
        RexInputRef ref, Geometry g2) {
      final Geometry g3 = SpatialTypeFunctions.ST_Envelope(g2);
      final Envelope env =  g3.getEnvelopeInternal();
      final HilbertCurve2D hilbert = new HilbertCurve2D(8);
      final List<SpaceFillingCurve2D.IndexRange> ranges =
          hilbert.toRanges(env.getMinX(), env.getMinY(), env.getMaxX(),
              env.getMaxY(), new SpaceFillingCurve2D.RangeComputeHints());
      final List<RexNode> nodes = new ArrayList<>();
      for (SpaceFillingCurve2D.IndexRange range : ranges) {
        final BigDecimal lowerBd = BigDecimal.valueOf(range.lower());
        final BigDecimal upperBd = BigDecimal.valueOf(range.upper());
        nodes.add(
            rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    ref,
                    rexBuilder.makeExactLiteral(lowerBd)),
                rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    ref,
                    rexBuilder.makeExactLiteral(upperBd))));
      }
      return rexBuilder.makeCall(SqlStdOperatorTable.OR, nodes);
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
      Config DEFAULT = ImmutableSpatialRules.Config.of()
          .withOperandSupplier(b ->
              b.operand(Filter.class)
                  .predicate(f -> DWITHIN_FINDER.inFilter(f)
                      && !HILBERT_FINDER.inFilter(f))
                  .anyInputs())
          .as(Config.class);

      @Override default FilterHilbertRule toRule() {
        return new FilterHilbertRule(this);
      }
    }
  }
}
