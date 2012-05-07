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

import org.eigenbase.rel.*;
import org.eigenbase.sql.*;


/**
 * OJRexImplementorTable contains, for each operator, an implementor which can
 * convert a call to that operator into OpenJava code.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface OJRexImplementorTable
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves the implementor of an operator, or null if there is no
     * implementor registered.
     */
    public OJRexImplementor get(SqlOperator op);

    /**
     * Retrieves the implementor of an aggregate, or null if there is no
     * implementor registered.
     */
    public OJAggImplementor get(Aggregation aggregation);
}

// End OJRexImplementorTable.java
