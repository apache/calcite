/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * Unit test for JSON data models.
 */
public class JsonModelTest extends ModelTest {
  @Override protected ObjectMapper getMapper() {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    return mapper;
  }

  @Override protected String getSimpleSchema() {
    return "{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'FoodMart',\n"
        + "       types: [\n"
        + "         {\n"
        + "           name: 'mytype1',\n"
        + "           attributes: [\n"
        + "             {\n"
        + "               name: 'f1',\n"
        + "               type: 'BIGINT'\n"
        + "             }\n"
        + "           ]\n"
        + "         }\n"
        + "       ],\n"
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
        + "}";
  }

  @Override protected String getSimpleSchemaWithSubtype() {
    return "{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       type: 'jdbc',\n"
        + "       name: 'FoodMart',\n"
        + "       jdbcUser: 'u_baz',\n"
        + "       jdbcPassword: 'p_baz',\n"
        + "       jdbcUrl: 'jdbc:baz',\n"
        + "       jdbcCatalog: 'cat_baz',\n"
        + "       jdbcSchema: ''\n"
        + "     }\n"
        + "   ]\n"
        + "}";
  }

  @Override protected String getCustomSchemaWithSubtype() {
    return "{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       type: 'custom',\n"
        + "       name: 'My Custom Schema',\n"
        + "       factory: 'com.acme.MySchemaFactory',\n"
        + "       operand: {a: 'foo', b: [1, 3.5] },\n"
        + "       tables: [\n"
        + "         { type: 'custom', name: 'T1' },\n"
        + "         { type: 'custom', name: 'T2', operand: {} },\n"
        + "         { type: 'custom', name: 'T3', operand: {a: 'foo'} }\n"
        + "       ]\n"
        + "     },\n"
        + "     {\n"
        + "       type: 'custom',\n"
        + "       name: 'has-no-operand'\n"
        + "     }\n"
        + "   ]\n"
        + "}";
  }

  @Override protected String getImmutableSchemaWithMaterialization() {
    return "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'adhoc',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'empty'\n"
        + "    },\n"
        + "    {\n"
        + "      name: 'adhoc',\n"
        + "      type: 'custom',\n"
        + "      factory: '"
        + JdbcTest.MySchemaFactory.class.getName()
        + "',\n"
        + "      operand: {\n"
        + "           'tableName': 'ELVIS',\n"
        + "           'mutable': false\n"
        + "      },\n"
        + "      materializations: [\n"
        + "        {\n"
        + "          table: 'v',\n"
        + "          sql: 'values (1)'\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }

  @Override protected String getSchemaWithoutName() {
    return "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'adhoc',\n"
        + "  schemas: [ {\n"
        + "  } ]\n"
        + "}";
  }

  @Override protected String getCustomSchemaWithoutFactory() {
    return "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'adhoc',\n"
        + "  schemas: [ {\n"
        + "    type: 'custom',\n"
        + "    name: 'my_custom_schema'\n"
        + "  } ]\n"
        + "}";
  }

  @Override protected String getLattice() {
    return "{\n"
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
        + "         },\n"
        + "         {\n"
        + "           name: 'V',\n"
        + "           type: 'view',\n"
        + "           sql: 'values (1)'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'V2',\n"
        + "           type: 'view',\n"
        + "           sql: [ 'values (1)', '(2)' ]\n"
        + "         }\n"
        + "       ],\n"
        + "       lattices: [\n"
        + "         {\n"
        + "           name: 'SalesStar',\n"
        + "           sql: 'select * from sales_fact_1997'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'SalesStar2',\n"
        + "           sql: [ 'select *', 'from sales_fact_1997' ]\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}";
  }

  @Override protected String getBadMultilineSql() {
    return "{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'FoodMart',\n"
        + "       tables: [\n"
        + "         {\n"
        + "           name: 'V',\n"
        + "           type: 'view',\n"
        + "           sql: [ 'values (1)', 2 ]\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}";
  }
}

// End JsonModelTest.java
