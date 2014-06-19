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

import net.hydromatic.linq4j.expressions.Expression;

import org.eigenbase.rex.RexNode;

import java.util.List;

/**
 * Implements a windowed aggregate function by generating expressions to
 * initialize, add to, and get a result from, an accumulator.
 * Windowed aggregate is more powerful than regular aggregate since it can
 * access rows in the current partition by row indices.
 * Regular aggregate can be used to implement windowed aggregate.
 *
 * @see net.hydromatic.optiq.rules.java.StrictWinAggImplementor
 * @see net.hydromatic.optiq.rules.java.RexImpTable.FirstLastValueImplementor
 * @see net.hydromatic.optiq.rules.java.RexImpTable.RankImplementor
 * @see net.hydromatic.optiq.rules.java.RexImpTable.RowNumberImplementor
 */
public interface WinAggImplementor extends AggImplementor {
  /**
   * Allows to access rows in window partition relative to first/last and
   * current row.
   */
  public enum SeekType {
    /**
     * Start of window.
     * @see WinAggFrameContext#startIndex()
     */
    START,
    /**
     * Row position in the frame.
     * @see WinAggFrameContext#index()
     */
    SET,
    /**
     * The index of row that is aggregated.
     * Valid only in {@link WinAggAddContext}.
     * @see WinAggAddContext#currentPosition()
     */
    AGG_INDEX,
    /**
     * End of window.
     * @see WinAggFrameContext#endIndex()
     */
    END
  }

  /**
   * Marker interface to allow {@link AggImplementor} to tell if it is used in
   * regular or windowed context.
   */
  public interface WinAggContext extends AggContext {
  }

  /**
   * Provides information on the current window.
   * All the indexes are ready to be used in {@link WinAggFrameResultContext#arguments(Expression)},
   * {@link WinAggFrameResultContext#rowTranslator(Expression)} and similar methods.
   */
  public interface WinAggFrameContext {
    /**
     * Returns the index of the current row in the partition.
     * In other words, it is close to ~ROWS BETWEEN CURRENT ROW.
     * Note to use {@link #startIndex()} when you need zero-based row position.
     * @return the index of the very first row in partition
     */
    Expression index();

    /**
     * Returns the index of the very first row in partition.
     * @return index of the very first row in partition
     */
    Expression startIndex();

    /**
     * Returns the index of the very last row in partition.
     * @return index of the very last row in partition
     */
    Expression endIndex();

    /**
     * Returns the boolean expression that tells if the partition has rows.
     * The partition might lack rows in cases like ROWS BETWEEN 1000 PRECEDING
     * AND 900 PRECEDING.
     * @return boolean expression that tells if the partition has rows
     */
    Expression hasRows();

    /**
     * Returns the number of rows in the current partition.
     * @return number of rows in the current partition or 0 if the partition
     *   is empty
     */
    Expression getPartitionRowCount();
  }

  /**
   * Provides information on the current window when computing the result of
   * the aggregation.
   */
  public interface WinAggFrameResultContext {
    /**
     * Returns {@link RexNode} representation of arguments.
     * This can be useful for manual translation of required arguments with
     * different {@link net.hydromatic.optiq.rules.java.NullPolicy}.
     * @return {@link RexNode} representation of arguments
     */
    List<RexNode> rexArguments();

    /**
     * Returns Linq4j form of arguments.
     * The resulting value is equivalent to
     * {@code rowTranslator().translateList(rexArguments())}.
     * This is handy if you need just operate on argument.
     * @param rowIndex index of the requested row. The index must be in range
     *                 of partition's startIndex and endIndex.
     * @return Linq4j form of arguments of the particular row
     */
    List<Expression> arguments(Expression rowIndex);

    /**
     * Converts absolute index position of the given relative position.
     * @param offset offset of the requested row
     * @param seekType the type of offset (start of window, end of window, etc)
     * @return absolute position of the requested row
     */
    Expression computeIndex(Expression offset, SeekType seekType);

    /**
     * Returns row translator for given absolute row position.
     * @param rowIndex absolute index of the row.
     * @return translator for the requested row
     */
    RexToLixTranslator rowTranslator(Expression rowIndex);

    /**
     * Compares two rows given by absolute positions according to the order
     * collation of the current window.
     * @param a absolute index of the first row
     * @param b absolute index of the second row
     * @return result of comparison as as in {@link Comparable#compareTo}
     */
    Expression compareRows(Expression a, Expression b);
  }
}

// End WinAggImplementor.java
