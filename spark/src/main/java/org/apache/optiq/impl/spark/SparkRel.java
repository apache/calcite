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
package org.apache.optiq.impl.spark;

import org.apache.linq4j.expressions.BlockStatement;

import org.apache.optiq.impl.enumerable.JavaRelImplementor;
import org.apache.optiq.impl.enumerable.PhysType;

import org.apache.optiq.rel.RelNode;
import org.apache.optiq.relopt.Convention;
import org.apache.optiq.rex.RexBuilder;

/**
 * Relational expression that uses Spark calling convention.
 */
public interface SparkRel extends RelNode {
  Result implementSpark(Implementor implementor);

  /** Calling convention for relational operations that occur in Spark. */
  Convention CONVENTION = new Convention.Impl("SPARK", SparkRel.class);

  /** Extension to {@link JavaRelImplementor} that can handle Spark relational
   * expressions. */
  public abstract class Implementor extends JavaRelImplementor {
    public Implementor(RexBuilder rexBuilder) {
      super(rexBuilder);
    }

    abstract Result result(PhysType physType, BlockStatement blockStatement);

    abstract Result visitInput(SparkRel parent, int ordinal, SparkRel input);
  }

  /** Result of generating Java code to implement a Spark relational
   * expression. */
  public class Result {
    public final BlockStatement block;
    public final PhysType physType;

    public Result(PhysType physType, BlockStatement block) {
      this.physType = physType;
      this.block = block;
    }
  }
}

// End SparkRel.java
