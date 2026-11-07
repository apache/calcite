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
package org.apache.calcite.sql2rel;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Set;

/** Conservatively analyzes references to correlation bindings outside a
 * relational tree without changing it.
 *
 * <p>Local bindings hide references only within their scope. Regions with
 * unknown expression coverage or binding scopes may contain outer references. */
final class CorrelationReferences {
  /** Correlation to inspect, or null to match any free correlation. */
  private final @Nullable CorrelationId targetId;

  /** Declared field dependencies to retain when a region cannot be analyzed. */
  private final ImmutableBitSet declared;

  /** Field ordinals found or conservatively retained during this analysis. */
  private final ImmutableBitSet.Builder columns = ImmutableBitSet.builder();

  /** Whether a matching outer reference was found or cannot be ruled out.
   * A zero-field row reference still counts as a use. */
  private boolean mayContainReference;

  private CorrelationReferences(@Nullable CorrelationId targetId, ImmutableBitSet declared) {
    this.targetId = targetId;
    this.declared = declared;
  }

  /** Returns whether the tree is known to have no references to outer bindings.
   * Unknown expression coverage or binding scopes prevent this conclusion. */
  static boolean isUncorrelated(RelNode root) {
    return !collect(root, null, ImmutableBitSet.of()).mayContainReference;
  }

  /** Returns whether the tree may use an outer binding with the given id.
   * References hidden by a local binding of the same id are excluded.
   * False means that no reference to this outer binding can occur. */
  static boolean mayUse(RelNode root, CorrelationId id) {
    return collect(root, id, ImmutableBitSet.of()).mayContainReference;
  }

  /** Returns the declared columns that may still be referenced through
   * the outer binding with the given id.
   * The declaration must cover all valid references to {@code id}. Only
   * columns known to be unused are removed; the whole declaration is retained
   * when a relevant region cannot be analyzed. */
  static ImmutableBitSet requiredColumns(RelNode root, CorrelationId id,
      ImmutableBitSet declared) {
    return declared.intersect(collect(root, id, declared).columns.build());
  }

  /** Starts a fresh analysis with no local bindings in scope. */
  private static CorrelationReferences collect(RelNode root, @Nullable CorrelationId id,
      ImmutableBitSet declared) {
    final CorrelationReferences result = new CorrelationReferences(id, declared);
    result.new ReferenceVisitor(ImmutableSet.of()).go(root);
    return result;
  }

  /** Visits one binding scope and accumulates results in the enclosing analysis. */
  private class ReferenceVisitor extends RelVisitor {
    /** Ids bound along this path inside the analyzed tree. */
    private final Set<CorrelationId> bound;

    ReferenceVisitor(Set<CorrelationId> bound) {
      this.bound = bound;
    }

    @Override public void visit(RelNode rel, int ordinal, @Nullable RelNode parent) {
      if (isTargetBound()) {
        return;
      }
      final Set<CorrelationId> local = rel.getVariablesSet();
      if (rel instanceof Correlate) {
        collectReferences(rel);
        visit(((Correlate) rel).getLeft(), 0, rel);
        withBindings(local).visit(((Correlate) rel).getRight(), 1, rel);
      } else if (rel instanceof Project || rel instanceof Filter) {
        withBindings(local).collectReferences(rel);
        super.visit(rel, ordinal, parent);
      } else if (!local.isEmpty()) {
        mayContainReference = true;
        columns.addAll(declared);
      } else {
        collectReferences(rel);
        super.visit(rel, ordinal, parent);
      }
    }

    /** Returns a visitor with additional local bindings in scope, sharing
     * the accumulated results. */
    private ReferenceVisitor withBindings(Set<CorrelationId> local) {
      return local.isEmpty() ? this
          : new ReferenceVisitor(
              ImmutableSet.<CorrelationId>builder().addAll(bound).addAll(local).build());
    }

    /** Returns whether the specific target is hidden by a local binding. */
    private boolean isTargetBound() {
      return targetId != null && bound.contains(targetId);
    }

    /** Returns whether the variable is a free reference matching the target id,
     * or any free reference when no target id was specified. */
    private boolean matches(RexCorrelVariable variable) {
      return !bound.contains(variable.id) && (targetId == null || targetId.equals(variable.id));
    }

    /** Collects possible outer references from this node's expressions and
     * their sub-query plans. */
    private void collectReferences(RelNode rel) {
      if (isTargetBound()) {
        return;
      }
      final RexShuttle visitor = new RexShuttle() {
        @Override public RexNode visitFieldAccess(RexFieldAccess access) {
          if (access.getReferenceExpr() instanceof RexCorrelVariable) {
            final RexCorrelVariable variable =
                (RexCorrelVariable) access.getReferenceExpr();
            if (matches(variable)) {
              mayContainReference = true;
              columns.set(access.getField().getIndex());
            }
            return access;
          }
          return super.visitFieldAccess(access);
        }

        @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
          if (matches(variable)) {
            mayContainReference = true;
            columns.addAll(ImmutableBitSet.range(variable.getType().getFieldCount()));
          }
          return variable;
        }

        @Override public RexNode visitSubQuery(RexSubQuery query) {
          ReferenceVisitor.this.visit(query.rel, 0, null);
          return super.visitSubQuery(query);
        }
      };
      if (!RelOptUtil.tryVisitExpressions(rel, visitor)) {
        mayContainReference = true;
        columns.addAll(declared);
      }
    }
  }
}
