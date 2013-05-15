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
package net.hydromatic.optiq.jdbc;

import net.hydromatic.linq4j.function.Function0;
import net.hydromatic.optiq.runtime.ColumnMetaData;
import net.hydromatic.optiq.runtime.Cursor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * Utility methods, mainly concerning error-handling.
 *
 * @author jhyde
 */
public class Helper {
    public static final Helper INSTANCE = new Helper();

    private Helper() {
    }

    public RuntimeException todo() {
        return new RuntimeException("todo: implement this method");
    }

    public RuntimeException wrap(String message, Exception e) {
        return new RuntimeException(message, e);
    }

    public SQLException createException(String message, Exception e) {
        return new SQLException(message, e);
    }

    public SQLException createException(String message) {
        return new SQLException(message);
    }

    public SQLException toSQLException(SQLException exception) {
        return exception;
    }

    /** Creates an empty result set. Useful for JDBC metadata methods that are
     * not implemented or which query entities that are not supported (e.g.
     * triggers in Lingual). */
    public ResultSet createEmptyResultSet(OptiqConnectionImpl connection) {
        try {
            return connection.driver.factory.newResultSet(
                connection.createStatement(),
                Collections.<ColumnMetaData>emptyList(),
                new Function0<Cursor>() {
                    public Cursor apply() {
                        return new Cursor() {
                            public List<Accessor> createAccessors(
                                List<ColumnMetaData> types)
                            {
                                assert types.isEmpty();
                                return Collections.emptyList();
                            }

                            public boolean next() {
                                return false;
                            }
                        };
                    }
                }).execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

// End Helper.java
