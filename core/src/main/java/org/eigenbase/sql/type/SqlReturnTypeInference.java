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
 * Strategy interface to infer the type of an operator call from the type of the
 * operands.
 *
 * <p>This interface is an example of the {@link
 * org.eigenbase.util.Glossary#StrategyPattern strategy pattern}. This makes
 * sense because many operators have similar, straightforward strategies, such
 * as to take the type of the first operand.</p>
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Sept 8, 2004
 */
public interface SqlReturnTypeInference
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Infers the return type of a call to an {@link SqlOperator}.
     *
     * @param opBinding description of operator binding
     *
     * @return inferred type; may be null
     */
    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding);
}

// End SqlReturnTypeInference.java
