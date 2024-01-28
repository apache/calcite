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
package org.apache.calcite.rel.hint;

import org.apache.calcite.rel.RelNode;

/**
 * A hint strategy whose rules are totally customized.
 *
 * @see ExplicitHintMatcher
 */
public class ExplicitHintStrategy implements HintStrategy {
  //~ Instance fields --------------------------------------------------------

  private final ExplicitHintMatcher matcher;

  /**
   * Creates an {@code ExplicitHintStrategy} with specified {@code matcher}.
   *
   * <p>Make this constructor package-protected intentionally, use
   * {@link HintStrategies#explicit(ExplicitHintMatcher)}.
   *
   * @param matcher ExplicitHintMatcher instance to test
   *                if a hint can be applied to a rel
   */
  ExplicitHintStrategy(ExplicitHintMatcher matcher) {
    this.matcher = matcher;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean canApply(RelHint hint, RelNode rel) {
    return this.matcher.matches(hint, rel);
  }
}
