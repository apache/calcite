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
package net.hydromatic.optiq.rules.java;

import net.hydromatic.linq4j.expressions.BlockExpression;

import org.eigenbase.rel.RelNode;


/**
 * A relational expression of one of the
 * {@link net.hydromatic.optiq.rules.java.EnumerableConvention} calling
 * conventions.
 */
public interface EnumerableRel
    extends RelNode
{
  //~ Methods ----------------------------------------------------------------

  /**
   * Creates a plan for this expression according to a calling convention.
   *
   * @param implementor implementor
   */
  BlockExpression implement(EnumerableRelImplementor implementor);

  /**
   * Describes the Java type returned by this relational expression, and the
   * mapping between it and the fields of the logical row type.
   */
  PhysType getPhysType();

}

// End EnumerableRel.java
