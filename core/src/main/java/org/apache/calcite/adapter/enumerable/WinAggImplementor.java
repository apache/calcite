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
package org.apache.calcite.adapter.enumerable;

/**
 * Implements a windowed aggregate function by generating expressions to
 * initialize, add to, and get a result from, an accumulator.
 * Windowed aggregate is more powerful than regular aggregate since it can
 * access rows in the current partition by row indices.
 * Regular aggregate can be used to implement windowed aggregate.
 * <p>This interface does not define new methods: window-specific
 * sub-interfaces are passed when implementing window aggregate.
 *
 * @see org.apache.calcite.adapter.enumerable.StrictWinAggImplementor
 * @see org.apache.calcite.adapter.enumerable.RexImpTable.FirstLastValueImplementor
 * @see org.apache.calcite.adapter.enumerable.RexImpTable.RankImplementor
 * @see org.apache.calcite.adapter.enumerable.RexImpTable.RowNumberImplementor
 */
public interface WinAggImplementor extends AggImplementor {
  /**
   * Allows to access rows in window partition relative to first/last and
   * current row.
   */
  enum SeekType {
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

  boolean needCacheWhenFrameIntact();
}

// End WinAggImplementor.java
