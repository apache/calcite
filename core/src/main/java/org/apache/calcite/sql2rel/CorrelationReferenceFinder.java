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
package org.apache.calcite.sql2rel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;

import java.util.function.Function;

/**
 * Shuttle that finds references to a given {@link CorrelationId} within a tree
 * of {@link RelNode}s.
 */
public class CorrelationReferenceFinder extends RelShuttleImpl {
  private final MyRexVisitor rexVisitor;

  /** Creates CorrelationReferenceFinder. */
  public CorrelationReferenceFinder(Function<RexFieldAccess, RexNode> handler) {
    rexVisitor = new MyRexVisitor(handler);
  }


  @Override public RelNode doLeave(RelNode other) {
    return other.accept(rexVisitor);
  }

  /**
   * Replaces alternative names of correlation variable to its canonical name.
   */
  private static class MyRexVisitor extends RexShuttle {
    private final Function<RexFieldAccess, RexNode> handler;

    private MyRexVisitor(Function<RexFieldAccess, RexNode> handler) {
      this.handler = handler;
    }

    @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
        return handler.apply(fieldAccess);
      }
      return super.visitFieldAccess(fieldAccess);
    }

    @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
      CorrelationReferenceFinder finder = new CorrelationReferenceFinder(handler);
      final RelNode r = subQuery.rel.accept(finder); // look inside sub-queries
      if (r != subQuery.rel) {
        subQuery = subQuery.clone(r);
      }
      return super.visitSubQuery(subQuery);
    }
  }
}

// End CorrelationReferenceFinder.java
