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
package org.apache.calcite.linq4j.tree;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a {@code try ... catch ... finally} block.
 */
public class TryStatement extends Statement {
  public final Statement body;
  public final List<CatchBlock> catchBlocks;
  public final Statement fynally;

  public TryStatement(Statement body, List<CatchBlock> catchBlocks,
      Statement fynally) {
    super(ExpressionType.Try, body.getType());
    this.body = Objects.requireNonNull(body);
    this.catchBlocks = Objects.requireNonNull(catchBlocks);
    this.fynally = fynally;
  }

  @Override public Statement accept(Shuttle shuttle) {
    shuttle = shuttle.preVisit(this);
    Statement body1 = body.accept(shuttle);
    List<CatchBlock> catchBlocks1 = new ArrayList<>();
    for (CatchBlock cb: catchBlocks) {
      Statement cbBody = cb.body.accept(shuttle);
      catchBlocks1.add(
          Expressions.catch_(cb.parameter, cbBody));
    }
    Statement fynally1 =
        fynally == null ? null : fynally.accept(shuttle);
    return shuttle.visit(this, body1, catchBlocks1, fynally1);
  }

  public <R> R accept(Visitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override void accept0(ExpressionWriter writer) {
    writer.append("try ").append(Blocks.toBlock(body));
    for (CatchBlock catchBlock : catchBlocks) {
      writer.backUp();
      writer.append(" catch (").append(catchBlock.parameter.declString())
          .append(") ").append(Blocks.toBlock(catchBlock.body));
    }
    if (fynally != null) {
      writer.backUp();
      writer.append(" finally ").append(Blocks.toBlock(fynally));
    }
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    TryStatement that = (TryStatement) o;

    if (!body.equals(that.body)) {
      return false;
    }
    if (!catchBlocks.equals(that.catchBlocks)) {
      return false;
    }
    if (fynally != null ? !fynally.equals(that.fynally) : that.fynally
        != null) {
      return false;
    }

    return true;
  }

  @Override public int hashCode() {
    return Objects.hash(nodeType, type, body, catchBlocks, fynally);
  }
}

// End TryStatement.java
