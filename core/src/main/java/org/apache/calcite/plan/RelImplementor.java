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
package org.eigenbase.relopt;

import org.eigenbase.rel.*;

/**
 * Callback used to hold state while converting a tree of {@link RelNode
 * relational expressions} into a plan. Calling conventions typically have their
 * own protocol for walking over a tree, and correspondingly have their own
 * implementors, which are subclasses of <code>RelImplementor</code>.
 */
public interface RelImplementor {
  //~ Methods ----------------------------------------------------------------

  /**
   * Implements a relational expression according to a calling convention.
   *
   * @param parent  Parent relational expression
   * @param ordinal Ordinal of child within its parent
   * @param child   Child relational expression
   * @return Interpretation of the return value is left to the implementor
   */
  Object visitChild(
      RelNode parent,
      int ordinal,
      RelNode child);

  /**
   * Called from {@link #visitChild} after the frame has been set up. Specific
   * implementors should override this method.
   *
   * @param child   Child relational expression
   * @param ordinal Ordinal of child within its parent
   * @param arg     Additional parameter; type depends on implementor
   * @return Interpretation of the return value is left to the implementor
   */
  Object visitChildInternal(RelNode child, int ordinal, Object arg);
}

// End RelImplementor.java
