/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.rules.java;

import net.hydromatic.linq4j.expressions.BlockBuilder;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;

import java.util.List;

/**
 * Information for a call to {@link AggImplementor#implementAdd(AggContext, AggAddContext)}.
 * {@link WinAggAddContext} is used when implementing windowed aggregate.
 * Note: logically, {@link WinAggAddContext} should extend {@link WinAggResultContext},
 * however this would prohibit usage of the same {@link AggImplementor} for both
 * regular aggregate and window aggregate.
 * Typically, the aggregation implementation will use {@link #arguments()}
 * or {@link #rexArguments()} to update aggregate value.
 * @see net.hydromatic.optiq.rules.java.AggAddContext
 */
public abstract class WinAggAddContext
    extends AggAddContext
    implements WinAggImplementor.WinAggFrameContext,
      WinAggImplementor.WinAggFrameResultContext {
  public WinAggAddContext(BlockBuilder block, List<Expression> accumulator) {
    super(block, accumulator);
  }

  /**
   * Returns current position inside for-loop of window aggregate.
   * Note, the position is relative to {@link WinAggImplementor.WinAggFrameContext#startIndex()}.
   * This is NOT current row as in "rows between current row".
   * If you need to know the relative index of the current row in the partition,
   * use {@link WinAggImplementor.WinAggFrameContext#index()}.
   * @return current position inside for-loop of window aggregate.
   * @see WinAggImplementor.WinAggFrameContext#index()
   * @see WinAggImplementor.WinAggFrameContext#startIndex()
   */
  public abstract Expression currentPosition();

  public List<Expression> arguments(Expression rowIndex) {
    return rowTranslator(rowIndex).translateList(rexArguments());
  }

  @Override
  public final RexToLixTranslator rowTranslator() {
    return rowTranslator(computeIndex(Expressions.constant(0),
        WinAggImplementor.SeekType.AGG_INDEX));
  }
}

// End WinAggAddContext.java
