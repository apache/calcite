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

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Represents creating a new array and possibly initializing the elements of the
 * new array.
 */
public class NewArrayExpression extends Expression {
    private final List<Expression> expressions;

    public NewArrayExpression(Type type, List<Expression> expressions) {
        super(ExpressionType.NewArrayInit, arrayClass(type));
        this.expressions = expressions;
    }

    private static Class arrayClass(Type type) {
        // REVIEW: Is there a way to do this without creating an instance? We
        //  just need the inverse of Class.getComponentType().
        return Array.newInstance(Types.toClass(type), 0).getClass();
    }

    @Override
    void accept(ExpressionWriter writer, int lprec, int rprec) {
        writer.append("new ")
            .append(type)
            .list(" {\n", ",\n", "}", expressions);
    }
}

// End NewArrayExpression.java
