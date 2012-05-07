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
package org.eigenbase.sql.validate;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;


/**
 * Supplies a {@link SqlValidator} with the metadata for a table.
 *
 * @author jhyde
 * @version $Id$
 * @see SqlValidatorCatalogReader
 * @since Mar 25, 2003
 */
public interface SqlValidatorTable
{
    //~ Methods ----------------------------------------------------------------

    RelDataType getRowType();

    String [] getQualifiedName();

    /**
     * Returns whether a given column is monotonic.
     */
    SqlMonotonicity getMonotonicity(String columnName);

    /**
     * Returns the access type of the table
     */
    SqlAccessType getAllowedAccess();
}

// End SqlValidatorTable.java
