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

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexShuttle;

/**
 * Dispatches a {@link RexShuttle} for all {@link RelNode}-s.
 */
public class RexRewritingRelShuttle extends RelHomogeneousShuttle {
  private final RexShuttle rexVisitor;

  public RexRewritingRelShuttle(RexShuttle rexVisitor) {
    this.rexVisitor = rexVisitor;
  }

  @Override public RelNode visit(RelNode other) {
    RelNode next = super.visit(other);
    return next.accept(rexVisitor);
  }
}
