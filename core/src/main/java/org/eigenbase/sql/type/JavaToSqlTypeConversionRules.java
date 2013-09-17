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

import java.math.*;

import java.sql.*;
import java.sql.Date;

import java.util.*;


/**
 * JavaToSqlTypeConversionRules defines mappings from common Java types to
 * corresponding SQL types.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JavaToSqlTypeConversionRules
{
    //~ Static fields/initializers ---------------------------------------------

    private static final JavaToSqlTypeConversionRules instance =
        new JavaToSqlTypeConversionRules();

    //~ Instance fields --------------------------------------------------------

    private final Map<Class<?>, SqlTypeName> rules =
        new HashMap<Class<?>, SqlTypeName>();

    //~ Constructors -----------------------------------------------------------

    private JavaToSqlTypeConversionRules()
    {
        rules.put(Integer.class, SqlTypeName.INTEGER);
        rules.put(int.class, SqlTypeName.INTEGER);
        rules.put(Long.class, SqlTypeName.BIGINT);
        rules.put(long.class, SqlTypeName.BIGINT);
        rules.put(Short.class, SqlTypeName.SMALLINT);
        rules.put(short.class, SqlTypeName.SMALLINT);
        rules.put(byte.class, SqlTypeName.TINYINT);
        rules.put(Byte.class, SqlTypeName.TINYINT);

        rules.put(Float.class, SqlTypeName.REAL);
        rules.put(float.class, SqlTypeName.REAL);
        rules.put(Double.class, SqlTypeName.DOUBLE);
        rules.put(double.class, SqlTypeName.DOUBLE);

        rules.put(boolean.class, SqlTypeName.BOOLEAN);
        rules.put(Boolean.class, SqlTypeName.BOOLEAN);
        rules.put(byte [].class, SqlTypeName.VARBINARY);
        rules.put(String.class, SqlTypeName.VARCHAR);
        rules.put(char [].class, SqlTypeName.VARCHAR);
        rules.put(Character.class, SqlTypeName.CHAR);
        rules.put(char.class, SqlTypeName.CHAR);

        rules.put(java.util.Date.class, SqlTypeName.TIMESTAMP);
        rules.put(Date.class, SqlTypeName.DATE);
        rules.put(Timestamp.class, SqlTypeName.TIMESTAMP);
        rules.put(Time.class, SqlTypeName.TIME);
        rules.put(BigDecimal.class, SqlTypeName.DECIMAL);

        rules.put(ResultSet.class, SqlTypeName.CURSOR);
        rules.put(ColumnList.class, SqlTypeName.COLUMN_LIST);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the {@link org.eigenbase.util.Glossary#SingletonPattern
     * singleton} instance.
     */
    public static JavaToSqlTypeConversionRules instance()
    {
        return instance;
    }

    /**
     * Returns a corresponding {@link SqlTypeName} for a given Java class.
     *
     * @param javaClass the Java class to lookup
     *
     * @return a corresponding SqlTypeName if found, otherwise null is returned
     */
    public SqlTypeName lookup(Class javaClass)
    {
        return rules.get(javaClass);
    }

    /**
     * Make this public when needed. To represent COLUMN_LIST SQL value, we need
     * a type distinguishable from {@link List} in user-defined types.
     */
    private interface ColumnList extends List {
    }
}

// End JavaToSqlTypeConversionRules.java
