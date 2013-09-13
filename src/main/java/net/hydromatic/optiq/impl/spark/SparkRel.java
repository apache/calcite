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
package net.hydromatic.optiq.impl.spark;

import net.hydromatic.linq4j.expressions.BlockStatement;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.rules.java.PhysType;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;

/**
 * Relational expression that uses Spark calling convention.
 */
public interface SparkRel extends RelNode {
  Result implementSpark(Implementor implementor);

  /** Calling convention for relational operations that occur in Spark. */
  final Convention CONVENTION = new Convention.Impl("SPARK", SparkRel.class);

  public interface Implementor {
    Result result(PhysType physType, BlockStatement blockStatement);

    JavaTypeFactory getTypeFactory();
  }

  public class Result {
  }
}

// End SparkRel.java
