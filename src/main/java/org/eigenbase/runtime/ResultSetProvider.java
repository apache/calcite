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
package org.eigenbase.runtime;

import java.sql.*;

/**
 * ResultSetProvider is an interface for supplying a result set, typically
 * for use where deferred ResultSet creation is required.
 *
 * @author John Sichi
 * @version $Id$
 */
public interface ResultSetProvider
{
    /**
     * @return result set to be used
     */
    public ResultSet getResultSet() throws SQLException;
}

// End ResultSetProvider.java
