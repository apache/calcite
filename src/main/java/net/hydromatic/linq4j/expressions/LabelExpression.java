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

/**
 * Represents a label, which can be put in any {@link Expression} context. If it
 * is jumped to, it will get the value provided by the corresponding
 * {@link GotoExpression}. Otherwise, it receives the value in
 * {@link #defaultValue}. If the Type equals {@link Void}, no value should be
 * provided.
 */
public class LabelExpression extends Statement {
    public final Expression defaultValue;

    public LabelExpression(Expression defaultValue, ExpressionType nodeType) {
        super(nodeType, Void.TYPE);
        this.defaultValue = defaultValue;
    }
}

// End LabelExpression.java
