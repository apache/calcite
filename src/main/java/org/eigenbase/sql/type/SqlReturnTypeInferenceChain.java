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
package org.eigenbase.sql.type;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * Strategy to infer the type of an operator call from the type of the operands
 * by using a series of {@link SqlReturnTypeInference} rules in a given order.
 * If a rule fails to find a return type (by returning NULL), next rule is tried
 * until there are no more rules in which case NULL will be returned.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlReturnTypeInferenceChain
    implements SqlReturnTypeInference
{
    //~ Instance fields --------------------------------------------------------

    private final SqlReturnTypeInference [] rules;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FallbackCascade from an array of rules
     *
     * @pre null!=rules
     * @pre null!=rules[i]
     * @pre rules.length > 0
     */
    public SqlReturnTypeInferenceChain(
        SqlReturnTypeInference [] rules)
    {
        Util.pre(null != rules, "null!=rules");
        Util.pre(rules.length > 0, "rules.length>0");
        for (int i = 0; i < rules.length; i++) {
            Util.pre(rules[i] != null, "transforms[i] != null");
        }
        this.rules = rules;
    }

    /**
     * Creates a FallbackCascade from two rules
     */
    public SqlReturnTypeInferenceChain(
        SqlReturnTypeInference rule1,
        SqlReturnTypeInference rule2)
    {
        this(new SqlReturnTypeInference[] { rule1, rule2 });
    }

    //~ Methods ----------------------------------------------------------------

    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding)
    {
        RelDataType ret = null;
        for (int i = 0; i < rules.length; i++) {
            SqlReturnTypeInference rule = rules[i];
            ret = rule.inferReturnType(opBinding);
            if (null != ret) {
                break;
            }
        }
        return ret;
    }
}

// End SqlReturnTypeInferenceChain.java
