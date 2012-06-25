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

import java.util.List;

/**
 * Represents indexing a property or array.
 */
public class IndexExpression extends Expression {
    private final Expression array;
    private final List<Expression> indexExpressions;

    public IndexExpression(Expression array, List<Expression> indexExpressions) {
        super(
            ExpressionType.ArrayIndex, Types.getComponentType(array.getType()));
        this.array = array;
        this.indexExpressions = indexExpressions;
        assert indexExpressions.size() >= 1;
    }

    @Override
    void accept(ExpressionWriter writer, int lprec, int rprec) {
        array.accept(writer, lprec, nodeType.lprec);
        writer.list("[", ", ", "]", indexExpressions);
    }
}

// End IndexExpression.java
