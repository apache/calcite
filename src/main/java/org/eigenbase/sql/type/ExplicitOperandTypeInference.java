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
package org.eigenbase.sql.type;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;


/**
 * ExplicitOperandTypeInferences implements {@link SqlOperandTypeInference} by
 * explicity supplying a type for each parameter.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class ExplicitOperandTypeInference
    implements SqlOperandTypeInference
{
    //~ Instance fields --------------------------------------------------------

    private final RelDataType [] paramTypes;

    //~ Constructors -----------------------------------------------------------

    public ExplicitOperandTypeInference(RelDataType [] paramTypes)
    {
        this.paramTypes = paramTypes;
    }

    //~ Methods ----------------------------------------------------------------

    public void inferOperandTypes(
        SqlCallBinding callBinding,
        RelDataType returnType,
        RelDataType [] operandTypes)
    {
        System.arraycopy(paramTypes, 0, operandTypes, 0, paramTypes.length);
    }
}

// End ExplicitOperandTypeInference.java
