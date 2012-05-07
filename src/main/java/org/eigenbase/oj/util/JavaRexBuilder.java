/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.eigenbase.oj.util;

import java.util.List;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;


/**
 * Extends {@link RexBuilder} to builds row-expressions including those
 * involving Java code.
 *
 * @author jhyde
 * @version $Id$
 * @see JavaRowExpression
 * @since Nov 23, 2003
 */
public class JavaRexBuilder
    extends RexBuilder
{
    //~ Instance fields --------------------------------------------------------

    OJTranslator translator = new OJTranslator();

    //~ Constructors -----------------------------------------------------------

    public JavaRexBuilder(RelDataTypeFactory typeFactory)
    {
        super(typeFactory);
    }

    //~ Methods ----------------------------------------------------------------

    public RexNode makeFieldAccess(
        RexNode exp,
        String fieldName)
    {
        if (exp instanceof JavaRowExpression) {
            JavaRowExpression jexp = (JavaRowExpression) exp;
            final FieldAccess fieldAccess =
                new FieldAccess(
                    jexp.getExpression(),
                    fieldName);
            return makeJava(jexp.env, fieldAccess);
        } else {
            return super.makeFieldAccess(exp, fieldName);
        }
    }

    /**
     * Creates a call to a Java method.
     *
     * @param exp Target of the method
     * @param methodName Name of the method
     * @param args Argument expressions; null means no arguments
     *
     * @return Method call
     */
    public RexNode createMethodCall(
        Environment env,
        RexNode exp,
        String methodName,
        List<RexNode> args)
    {
        ExpressionList ojArgs = translator.toJava(args);
        Expression ojExp = translator.toJava(exp);
        return makeJava(
            env,
            new MethodCall(ojExp, methodName, ojArgs));
    }

    public RexNode makeJava(
        Environment env,
        Expression expr)
    {
        final OJClass ojClass;
        try {
            ojClass = expr.getType(env);
        } catch (Exception e) {
            throw Util.newInternal(
                e,
                "Error deriving type of expression " + expr);
        }
        RelDataType type = OJUtil.ojToType(this.typeFactory, ojClass);
        return new JavaRowExpression(env, type, expr);
    }

    public RexNode makeCase(
        RexNode rexCond,
        RexNode rexTrueCase,
        RexNode rexFalseCase)
    {
        throw Util.needToImplement(this);
    }

    public RexNode makeCast(
        RelDataType type,
        RexNode exp)
    {
        if (exp instanceof JavaRowExpression) {
            JavaRowExpression java = (JavaRowExpression) exp;
            final OJClass ojClass = OJUtil.typeToOJClass(type, typeFactory);
            final CastExpression castExpr =
                new CastExpression(
                    ojClass,
                    java.getExpression());
            return new JavaRowExpression(java.env, type, castExpr);
        }
        return super.makeCast(type, exp);
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class OJTranslator
    {
        public ExpressionList toJava(List<RexNode> args)
        {
            return null;
        }

        public Expression toJava(RexNode exp)
        {
            throw Util.needToImplement(this);
        }
    }
}

// End JavaRexBuilder.java
