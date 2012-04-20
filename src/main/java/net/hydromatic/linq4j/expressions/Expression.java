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
package net.hydromatic.linq4j.expressions;

import net.hydromatic.linq4j.Extensions;

/**
 * <p>Analogous to LINQ's System.Linq.Expression.</p>
 */
public abstract class Expression {
    public final ExpressionType nodeType;

    /**
     * Creates an Expression.
     *
     * @param nodeType Node type
     */
    public Expression(ExpressionType nodeType) {
        this.nodeType = nodeType;
    }

    /** Indicates that the node can be reduced to a simpler node. If this
     * returns true, Reduce() can be called to produce the reduced form. */
    public boolean canReduce() { throw Extensions.todo(); }

    /** Gets the node type of this Expression. */
    public ExpressionType getNodeType() {
        return nodeType;
    }

    /** Gets the static type of the expression that this Expression represents. */
    public Class getType() { throw Extensions.todo(); }
}

// End Expression.java
