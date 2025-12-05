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
package org.apache.calcite.rel;

import org.apache.calcite.plan.Context;

import org.apiguardian.api.API;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;

/**
 * Suggester for finding and returning interesting expressions that appear more than once in a
 * query. The notion of "interesting" is specific to the actual implementation of this interface.
 *
 * <p>In some cases the interesting expressions may be readily available in the input query while in
 * others they could be revealed after applying various transformations and algebraic
 * equivalences.
 *
 * <p>The final decision about using (or not) the suggestions provided by this class lies to the
 * query optimizer. For various reasons (e.g, incorrect or expensive expressions) the optimizer may
 * choose to reject/ignore the results provided by this class.
 *
 * <p>Implementations of this interface must have a public no argument constructor to allow
 * instantiation via reflection.
 *
 * <p>The interface is experimental and subject to change without notice.
 */
@API(since = "1.41.0", status = API.Status.EXPERIMENTAL)
public interface RelCommonExpressionSuggester {
  /**
   * Suggests interesting expressions for the specified input expression and context.
   *
   * @param input a relational expression representing the query under compilation.
   * @param context a context for tuning aspects of the suggestion process.
   * @return a collection with interesting expressions for the specified relational expression
   */
  Collection<RelNode> suggest(RelNode input, @Nullable Context context);
}
