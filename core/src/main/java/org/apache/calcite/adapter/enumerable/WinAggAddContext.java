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

import org.apache.calcite.linq4j.tree.Expression;

/**
 * Information for a call to
 * {@link AggImplementor#implementAdd(AggContext, AggAddContext)}.
 *
 * <p>{@link WinAggAddContext} is used when implementing windowed aggregate.
 * Typically, the aggregation implementation will use {@link #arguments()}
 * or {@link #rexArguments()} to update aggregate value.
 * @see AggAddContext
 */
public interface WinAggAddContext extends AggAddContext, WinAggResultContext {
  /**
   * Returns current position inside for-loop of window aggregate.
   * Note, the position is relative to {@link WinAggFrameContext#startIndex()}.
   * This is NOT current row as in "rows between current row".
   * If you need to know the relative index of the current row in the partition,
   * use {@link WinAggFrameContext#index()}.
   * @return current position inside for-loop of window aggregate.
   * @see WinAggFrameContext#index()
   * @see WinAggFrameContext#startIndex()
   */
  Expression currentPosition();
}

// End WinAggAddContext.java
