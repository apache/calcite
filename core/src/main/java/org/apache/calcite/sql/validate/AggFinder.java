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
package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;

/** Visitor that looks for an aggregate function inside a tree of
 * {@link SqlNode} objects and throws {@link Util.FoundOne} when it finds
 * one. */
class AggFinder extends AggVisitor {
  /**
   * Creates an AggFinder.
   *
   * @param opTab Operator table
   * @param over Whether to find windowed function calls {@code agg(x) OVER
   *             windowSpec}
   * @param aggregate Whether to find non-windowed aggregate calls
   * @param group Whether to find group functions (e.g. {@code TUMBLE})
   * @param delegate Finder to which to delegate when processing the arguments
   * @param nameMatcher Whether to match the agg function case-sensitively
   */
  AggFinder(SqlOperatorTable opTab, boolean over, boolean aggregate,
      boolean group, AggFinder delegate, SqlNameMatcher nameMatcher) {
    super(opTab, over, aggregate, group, delegate, nameMatcher);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Finds an aggregate.
   *
   * @param node Parse tree to search
   * @return First aggregate function in parse tree, or null if not found
   */
  public SqlCall findAgg(SqlNode node) {
    try {
      node.accept(this);
      return null;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return (SqlCall) e.getNode();
    }
  }

  public SqlCall findAgg(List<SqlNode> nodes) {
    try {
      for (SqlNode node : nodes) {
        node.accept(this);
      }
      return null;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return (SqlCall) e.getNode();
    }
  }

  protected Void found(SqlCall call) {
    throw new Util.FoundOne(call);
  }

  /** Creates a copy of this finder that has the same parameters as this,
   * then returns the list of all aggregates found. */
  Iterable<SqlCall> findAll(Iterable<SqlNode> nodes) {
    final AggIterable aggIterable =
        new AggIterable(opTab, over, aggregate, group, delegate, nameMatcher);
    for (SqlNode node : nodes) {
      node.accept(aggIterable);
    }
    return aggIterable.calls;
  }

  /** Iterates over all aggregates. */
  static class AggIterable extends AggVisitor implements Iterable<SqlCall> {
    private final List<SqlCall> calls = new ArrayList<>();

    AggIterable(SqlOperatorTable opTab, boolean over, boolean aggregate,
        boolean group, AggFinder delegate, SqlNameMatcher nameMatcher) {
      super(opTab, over, aggregate, group, delegate, nameMatcher);
    }

    @Override protected Void found(SqlCall call) {
      calls.add(call);
      return null;
    }

    @Nonnull public Iterator<SqlCall> iterator() {
      return calls.iterator();
    }
  }
}

// End AggFinder.java
