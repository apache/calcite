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
package org.eigenbase.oj.rex;

import openjava.ptree.*;

import org.eigenbase.rex.*;


/**
 * OJRexUnaryExpressionImplementor implements {@link OJRexImplementor} for row
 * expressions which can be translated to instances of OpenJava {@link
 * UnaryExpression}.
 *
 * @author Angel Chang
 * @version $Id$
 */
public class OJRexUnaryExpressionImplementor
    implements OJRexImplementor
{
    //~ Instance fields --------------------------------------------------------

    private final int ojUnaryExpressionOrdinal;

    //~ Constructors -----------------------------------------------------------

    public OJRexUnaryExpressionImplementor(int ojUnaryExpressionOrdinal)
    {
        this.ojUnaryExpressionOrdinal = ojUnaryExpressionOrdinal;
    }

    //~ Methods ----------------------------------------------------------------

    public Expression implement(
        RexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        return new UnaryExpression(operands[0], ojUnaryExpressionOrdinal);
    }

    public boolean canImplement(RexCall call)
    {
        return true;
    }
}

// End OJRexUnaryExpressionImplementor.java
