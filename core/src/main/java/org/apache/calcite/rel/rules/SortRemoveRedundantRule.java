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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;

import org.immutables.value.Value;

import java.util.Optional;

/**
 * Rule that removes redundant {@code Order By} or {@code Limit}
 * when its input RelNode's max row count is less than or equal to specified row count.
 * All of them are represented by {@link Sort}
 *
 * <p> If a {@code Sort} is order by,and its offset is null,when its input RelNode's
 * max row count is less than or equal to 1,then we could remove the redundant sort.
 *
 * <p> For example:
 * <blockquote><pre>{@code
 *  select max(totalprice) from orders order by 1}
 *  </pre></blockquote>
 *
 * <p> could be converted to
 * <blockquote><pre>{@code
 *  select max(totalprice) from orders}
 *  </pre></blockquote>
 *
 * <p> For example:
 * <blockquote><pre>{@code
 *  SELECT count(*) FROM orders ORDER BY 1 LIMIT 10 }
 *  </pre></blockquote>
 *
 * <p> could be converted to
 * <blockquote><pre>{@code
 *  SELECT count(*) FROM orders}
 *  </pre></blockquote>
 *
 * <p> If a {@code Sort} is pure limit,and its offset is null, when its input
 * RelNode's max row count is less than or equal to the limit's fetch,then we could
 * remove the redundant {@code Limit}.
 *
 * <p> For example:
 * <blockquote><pre>{@code
 * SELECT * FROM (VALUES 1,2,3,4,5,6) AS t1 LIMIT 10}
 * </pre></blockquote>
 *
 * <p> The above values max row count is 6 rows, and the limit's fetch is 10,
 * so we could remove the redundant sort.
 *
 * <p> It could be converted to:
 * <blockquote><pre>{@code
 * SELECT * FROM (VALUES 1,2,3,4,5,6) AS t1}
 * </pre></blockquote>
 *
 * @see CoreRules#SORT_REMOVE_REDUNDANT
 */
@Value.Enclosing
public class SortRemoveRedundantRule
    extends RelRule<SortRemoveRedundantRule.Config>
    implements TransformationRule {
  protected SortRemoveRedundantRule(final SortRemoveRedundantRule.Config config) {
    super(config);
  }

  @Override public void onMatch(final RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    if (RelOptUtil.isOffset(sort)) {
      // Don't remove sort if it has explicit OFFSET
      return;
    }

    // Get the max row count for sort's input RelNode.
    final Double inputMaxRowCount = call.getMetadataQuery().getMaxRowCount(sort.getInput());

    // Get the target max row count with sort's semantics.
    // If sort is 'order by x' or 'order by x limit n', the target max row count is 1.
    // If sort is pure limit, the target max row count is the limit's fetch.
    final Optional<Double> targetMaxRowCount = getSortInputSpecificMaxRowCount(sort);

    if (!targetMaxRowCount.isPresent()) {
      return;
    }

    // If the max row count is not null and less than or equal to targetMaxRowCount,
    // then we could remove the redundant sort.
    if (inputMaxRowCount != null && inputMaxRowCount <= targetMaxRowCount.get()) {
      call.transformTo(sort.getInput());
    }
  }

  private Optional<Double> getSortInputSpecificMaxRowCount(Sort sort) {
    // If the sort is pure limit, the specific max row count is limit's fetch.
    if (RelOptUtil.isPureLimit(sort)) {
      final double limit =
          sort.fetch instanceof RexLiteral ? RexLiteral.intValue(sort.fetch) : -1D;
      return Optional.of(limit);
    } else if (RelOptUtil.isOrder(sort)) {
      // If the sort is 'order by x' or 'order by x limit n', the specific max row count is 1.
      return Optional.of(1D);
    }
    return Optional.empty();
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableSortRemoveRedundantRule.Config.of()
        .withOperandSupplier(b ->
            b.operand(Sort.class).anyInputs());

    @Override default SortRemoveRedundantRule toRule() {
      return new SortRemoveRedundantRule(this);
    }
  }
}
