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
 * by using one {@link SqlReturnTypeInference} rule and a combination of {@link
 * SqlTypeTransform}s
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlTypeTransformCascade
    implements SqlReturnTypeInference
{
    //~ Instance fields --------------------------------------------------------

    private final SqlReturnTypeInference rule;
    private final SqlTypeTransform [] transforms;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a SqlTypeTransformCascade from a rule and an array of one or more
     * transforms.
     *
     * @pre null!=rule
     * @pre null!=transforms
     * @pre transforms.length > 0
     * @pre transforms[i] != null
     */
    public SqlTypeTransformCascade(
        SqlReturnTypeInference rule,
        SqlTypeTransform [] transforms)
    {
        Util.pre(null != rule, "null!=rule");
        Util.pre(null != transforms, "null!=transforms");
        Util.pre(transforms.length > 0, "transforms.length>0");
        for (int i = 0; i < transforms.length; i++) {
            Util.pre(transforms[i] != null, "transforms[i] != null");
        }
        this.rule = rule;
        this.transforms = transforms;
    }

    /**
     * Creates a SqlTypeTransformCascade from a rule and a single transform.
     *
     * @pre null!=rule
     * @pre null!=transform
     */
    public SqlTypeTransformCascade(
        SqlReturnTypeInference rule,
        SqlTypeTransform transform)
    {
        this(
            rule,
            new SqlTypeTransform[] { transform });
    }

    /**
     * Creates a SqlTypeTransformCascade from a rule and two transforms.
     *
     * @pre null!=rule
     * @pre null!=transform0
     * @pre null!=transform1
     */
    public SqlTypeTransformCascade(
        SqlReturnTypeInference rule,
        SqlTypeTransform transform0,
        SqlTypeTransform transform1)
    {
        this(
            rule,
            new SqlTypeTransform[] { transform0, transform1 });
    }

    //~ Methods ----------------------------------------------------------------

    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding)
    {
        RelDataType ret = rule.inferReturnType(opBinding);
        for (int i = 0; i < transforms.length; i++) {
            SqlTypeTransform transform = transforms[i];
            ret = transform.transformType(opBinding, ret);
        }
        return ret;
    }
}

// End SqlTypeTransformCascade.java
