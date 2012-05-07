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

import java.util.*;

import org.eigenbase.reltype.*;


/**
 * SqlTypeExplicitPrecedenceList implements the {@link
 * RelDataTypePrecedenceList} interface via an explicit list of SqlTypeName
 * entries.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlTypeExplicitPrecedenceList
    implements RelDataTypePrecedenceList
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Map from SqlTypeName to corresponding precedence list.
     *
     * @sql.2003 Part 2 Section 9.5
     */
    private static final Map<SqlTypeName, SqlTypeExplicitPrecedenceList>
        typeNameToPrecedenceList;

    static {
        // NOTE jvs 25-Jan-2005:  the null entries delimit equivalence
        // classes
        List<SqlTypeName> numericList =
            Arrays.asList(
                SqlTypeName.TINYINT,
                null,
                SqlTypeName.SMALLINT,
                null,
                SqlTypeName.INTEGER,
                null,
                SqlTypeName.BIGINT,
                null,
                SqlTypeName.DECIMAL,
                null,
                SqlTypeName.REAL,
                null,
                SqlTypeName.FLOAT,
                SqlTypeName.DOUBLE);
        typeNameToPrecedenceList =
            new HashMap<SqlTypeName, SqlTypeExplicitPrecedenceList>();
        addList(
            SqlTypeName.BOOLEAN,
            new SqlTypeName[] { SqlTypeName.BOOLEAN });
        addNumericList(
            SqlTypeName.TINYINT,
            numericList);
        addNumericList(
            SqlTypeName.SMALLINT,
            numericList);
        addNumericList(
            SqlTypeName.INTEGER,
            numericList);
        addNumericList(
            SqlTypeName.BIGINT,
            numericList);
        addNumericList(
            SqlTypeName.DECIMAL,
            numericList);
        addNumericList(
            SqlTypeName.REAL,
            numericList);
        addNumericList(
            SqlTypeName.FLOAT,
            numericList);
        addNumericList(
            SqlTypeName.DOUBLE,
            numericList);
        addList(
            SqlTypeName.CHAR,
            new SqlTypeName[] { SqlTypeName.CHAR, SqlTypeName.VARCHAR });
        addList(
            SqlTypeName.VARCHAR,
            new SqlTypeName[] { SqlTypeName.VARCHAR });
        addList(
            SqlTypeName.BINARY,
            new SqlTypeName[] { SqlTypeName.BINARY, SqlTypeName.VARBINARY });
        addList(
            SqlTypeName.VARBINARY,
            new SqlTypeName[] { SqlTypeName.VARBINARY });
        addList(
            SqlTypeName.DATE,
            new SqlTypeName[] { SqlTypeName.DATE });
        addList(
            SqlTypeName.TIME,
            new SqlTypeName[] { SqlTypeName.TIME });
        addList(
            SqlTypeName.TIMESTAMP,
            new SqlTypeName[] { SqlTypeName.TIMESTAMP });
        addList(
            SqlTypeName.INTERVAL_YEAR_MONTH,
            new SqlTypeName[] { SqlTypeName.INTERVAL_YEAR_MONTH });
        addList(
            SqlTypeName.INTERVAL_DAY_TIME,
            new SqlTypeName[] { SqlTypeName.INTERVAL_DAY_TIME });
    }

    //~ Instance fields --------------------------------------------------------

    private final List<SqlTypeName> typeNames;

    //~ Constructors -----------------------------------------------------------

    public SqlTypeExplicitPrecedenceList(SqlTypeName [] typeNames)
    {
        this.typeNames = Arrays.asList(typeNames);
    }

    //~ Methods ----------------------------------------------------------------

    private static void addList(
        SqlTypeName typeName,
        SqlTypeName [] array)
    {
        typeNameToPrecedenceList.put(
            typeName,
            new SqlTypeExplicitPrecedenceList(array));
    }

    private static void addNumericList(
        SqlTypeName typeName,
        List<SqlTypeName> numericList)
    {
        int i = getListPosition(typeName, numericList);
        List<SqlTypeName> subList = numericList.subList(i, numericList.size());
        SqlTypeName [] array = subList.toArray(new SqlTypeName[subList.size()]);
        addList(typeName, array);
    }

    // implement RelDataTypePrecedenceList
    public boolean containsType(RelDataType type)
    {
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return false;
        }
        return typeNames.contains(typeName);
    }

    // implement RelDataTypePrecedenceList
    public int compareTypePrecedence(RelDataType type1, RelDataType type2)
    {
        assert (containsType(type1));
        assert (containsType(type2));

        int p1 =
            getListPosition(
                type1.getSqlTypeName(),
                typeNames);
        int p2 =
            getListPosition(
                type2.getSqlTypeName(),
                typeNames);
        return p2 - p1;
    }

    private static int getListPosition(SqlTypeName type, List<SqlTypeName> list)
    {
        int i = list.indexOf(type);
        assert (i != -1);

        // adjust for precedence equivalence classes
        for (int j = i - 1; j >= 0; --j) {
            if (list.get(j) == null) {
                return j;
            }
        }
        return i;
    }

    static RelDataTypePrecedenceList getListForType(RelDataType type)
    {
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return null;
        }
        return typeNameToPrecedenceList.get(typeName);
    }
}

// End SqlTypeExplicitPrecedenceList.java
