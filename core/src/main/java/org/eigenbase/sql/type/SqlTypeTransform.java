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

/**
 * Strategy to transform one type to another. The transformation is dependent on
 * the implemented strategy object and in the general case is a function of the
 * type and the other operands. Can not be used by itself. Must be used in an
 * object of type {@link SqlTypeTransformCascade}.
 *
 * <p>This class is an example of the {@link
 * org.eigenbase.util.Glossary#StrategyPattern strategy pattern}.</p>
 *
 * @author Wael Chatila
 * @version $Id$
 */
public interface SqlTypeTransform
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Transforms a type.
     *
     * @param opBinding call context in which transformation is being performed
     * @param typeToTransform type to be transformed, never null
     *
     * @return transformed type, never null
     */
    public RelDataType transformType(
        SqlOperatorBinding opBinding,
        RelDataType typeToTransform);
}

// End SqlTypeTransform.java
