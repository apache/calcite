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
package org.eigenbase.oj.rex;

import openjava.ptree.*;

import org.eigenbase.rex.*;
import org.eigenbase.sql.*;


/**
 * OJRexBinaryExpressionImplementor implements {@link OJRexImplementor} for row
 * expressions which can be translated to instances of OpenJava {@link
 * BinaryExpression}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class OJRexBinaryExpressionImplementor
    implements OJRexImplementor
{
    //~ Instance fields --------------------------------------------------------

    private final int ojBinaryExpressionOrdinal;

    //~ Constructors -----------------------------------------------------------

    public OJRexBinaryExpressionImplementor(int ojBinaryExpressionOrdinal)
    {
        this.ojBinaryExpressionOrdinal = ojBinaryExpressionOrdinal;
    }

    //~ Methods ----------------------------------------------------------------

    public Expression implement(
        RexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        return new BinaryExpression(
            operands[0],
            ojBinaryExpressionOrdinal,
            operands[1]);
    }

    public boolean canImplement(RexCall call)
    {
        return true;
    }
}

// End OJRexBinaryExpressionImplementor.java
