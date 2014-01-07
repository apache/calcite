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
package org.eigenbase.sql.validate;

import org.eigenbase.sql.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.util.*;

/**
 * Visitor which looks for an aggregate function inside a tree of {@link
 * SqlNode} objects.
 */
class AggFinder extends SqlBasicVisitor<Void> {
  //~ Instance fields --------------------------------------------------------

  private final boolean over;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AggFinder.
   *
   * @param over Whether to find windowed function calls {@code Agg(x) OVER
   *             windowSpec}
   */
  AggFinder(boolean over) {
    this.over = over;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Finds an aggregate.
   *
   * @param node Parse tree to search
   * @return First aggregate function in parse tree, or null if not found
   */
  public SqlNode findAgg(SqlNode node) {
    try {
      node.accept(this);
      return null;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return (SqlNode) e.getNode();
    }
  }

  public Void visit(SqlCall call) {
    if (call.getOperator().isAggregator()) {
      throw new Util.FoundOne(call);
    }
    if (call.isA(SqlKind.QUERY)) {
      // don't traverse into queries
      return null;
    }
    if (call.getKind() == SqlKind.OVER) {
      if (over) {
        throw new Util.FoundOne(call);
      } else {
        // an aggregate function over a window is not an aggregate!
        return null;
      }
    }
    return super.visit(call);
  }
}

// End AggFinder.java
