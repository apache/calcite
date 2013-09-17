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
package org.eigenbase.sql.util;

import org.eigenbase.sql.SqlDialect;

/**
 * String that represents a kocher SQL statement, expression, or fragment.
 *
 * <p>A SqlString just contains a regular Java string, but the SqlString wrapper
 * indicates that the string has been created carefully guarding against all SQL
 * dialect and injection issues.
 *
 * <p>The easiest way to do build a SqlString is to use a {@link SqlBuilder}.
 *
 * @version $Id$
 * @author jhyde
 */
public class SqlString
{
    private final String s;
    private SqlDialect dialect;

    /**
     * Creates a SqlString.
     *
     * @param s Contents of string
     */
    public SqlString(SqlDialect dialect, String s)
    {
        this.dialect = dialect;
        this.s = s;
        assert s != null;
        assert dialect != null;
    }

    @Override
    public int hashCode()
    {
        return s.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof SqlString
           && s.equals(((SqlString) obj).s);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns the SQL string.
     *
     * @see #getSql()
     * @return SQL string
     */
    @Override
    public String toString()
    {
        return s;
    }

    /**
     * Returns the SQL string.
     * @return SQL string
     */
    public String getSql()
    {
        return s;
    }

    /**
     * Returns the dialect.
     */
    public SqlDialect getDialect()
    {
        return dialect;
    }
}

// End SqlString.java
