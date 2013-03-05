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
package net.hydromatic.optiq.test;

import net.hydromatic.optiq.model.*;

import junit.framework.TestCase;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Unit test for data models.
 */
public class ModelTest extends TestCase {
    /** Reads a simple schema from a string into objects. */
    public void testRead() throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        Object root0 = mapper.readValue(
            "{\n"
            + "  version: '1.0',\n"
            + "   schemas: [\n"
            + "     {\n"
            + "       name: 'FoodMart',\n"
            + "       tables: [\n"
            + "         {\n"
            + "           name: 'time_by_day',\n"
            + "           columns: [\n"
            + "             {\n"
            + "               name: 'time_id'\n"
            + "             }\n"
            + "           ]\n"
            + "         },\n"
            + "         {\n"
            + "           name: 'sales_fact_1997',\n"
            + "           columns: [\n"
            + "             {\n"
            + "               name: 'time_id'\n"
            + "             }\n"
            + "           ]\n"
            + "         }\n"
            + "       ]\n"
            + "     }\n"
            + "   ]\n"
            + "}",
            JsonRoot.class);
        assertTrue(root0 instanceof JsonRoot);
        JsonRoot root = (JsonRoot) root0;
        assertEquals("1.0", root.version);
        assertEquals(1, root.schemas.size());
        final JsonMapSchema schema = (JsonMapSchema) root.schemas.get(0);
        assertEquals("FoodMart", schema.name);
        assertEquals(2, schema.tables.size());
        final JsonTable table0 = schema.tables.get(0);
        assertEquals("time_by_day", table0.name);
        final JsonTable table1 = schema.tables.get(1);
        assertEquals("sales_fact_1997", table1.name);
        assertEquals(1, table0.columns.size());
        final JsonColumn column = table0.columns.get(0);
        assertEquals("time_id", column.name);
    }

    /** Reads a simple schema containing JdbcSchema, a sub-type of Schema. */
    public void testSubtype() throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        Object root0 = mapper.readValue(
            "{\n"
            + "  version: '1.0',\n"
            + "   schemas: [\n"
            + "     {\n"
            + "       type: 'jdbc',\n"
            + "       name: 'FoodMart',\n"
            + "       jdbcUser: 'foodmart',\n"
            + "       jdbcPassword: 'foodmart',\n"
            + "       jdbcUrl: 'jdbc:mysql://localhost',\n"
            + "       jdbcCatalog: 'foodmart',\n"
            + "       jdbcSchema: ''\n"
            + "     }\n"
            + "   ]\n"
            + "}",
            JsonRoot.class);
        assertTrue(root0 instanceof JsonRoot);
        JsonRoot root = (JsonRoot) root0;
        assertEquals("1.0", root.version);
        assertEquals(1, root.schemas.size());
        final JsonJdbcSchema schema = (JsonJdbcSchema) root.schemas.get(0);
        assertEquals("FoodMart", schema.name);
    }
}

// End ModelTest.java
