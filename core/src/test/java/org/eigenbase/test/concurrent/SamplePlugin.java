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
package org.eigenbase.test.concurrent;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

/**
 * Sample mtsql plugin. To use add at start of script
 * "@plugin org.eigenbase.test.concurrent.SamplePlugin".  After doing a prepare
 * you can then do "@describeResultSet" to show columns returned by query.
 */
public class SamplePlugin extends ConcurrentTestPlugin {
    private static final String DESCRIBE_RESULT_SET_CMD = "@describeResultSet";

    public ConcurrentTestPluginCommand getCommandFor(String name, String params)
    {
        if (name.equals(DESCRIBE_RESULT_SET_CMD)) {
            return new DescribeResultSet();
        }
        assert (false);
        return null;
    }

    public Iterable<String> getSupportedThreadCommands()
    {
        return Arrays.asList(new String[] { DESCRIBE_RESULT_SET_CMD });
    }

    static class DescribeResultSet implements ConcurrentTestPluginCommand {

        public void execute(TestContext testContext) throws IOException
        {
            Statement stmt =
                (PreparedStatement) testContext.getCurrentStatement();
            if (stmt == null) {
                testContext.storeMessage("No current statement");
            } else if (!(stmt instanceof PreparedStatement)) {
            } else {
                try {
                    ResultSetMetaData metadata =
                        ((PreparedStatement) stmt).getMetaData();
                    for (int i = 1; i <= metadata.getColumnCount(); i++) {
                        testContext.storeMessage(
                            metadata.getColumnName(i) + ": "
                            + metadata.getColumnTypeName(i));
                    }
                } catch (SQLException e) {
                    throw new IllegalStateException(e.toString());
                }
            }
        }
    }
}
// End SamplePlugin.java
