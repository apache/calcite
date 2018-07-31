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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;


/**
 * Unit test for YAML data models.
 */
public class YamlModelTest extends ModelTest {
  @Override protected ObjectMapper getMapper() {
    return new YAMLMapper();
  }

  @Override protected String getSimpleSchema() {
    return "version: 1.0\n"
        + "schemas: \n"
        + "- name: FoodMart\n"
        + "  types: \n"
        + "  - name: mytype1\n"
        + "    attributes: \n"
        + "    - name: f1\n"
        + "      type: BIGINT\n"
        + "  tables: \n"
        + "  - name: time_by_day\n"
        + "    columns: \n"
        + "    - name: time_id\n"
        + "  - name: sales_fact_1997\n"
        + "    columns: \n"
        + "    - name: time_id\n";
  }

  @Override protected String getSimpleSchemaWithSubtype() {
    return "version: 1.0\n"
        + "schemas: \n"
        + "- type: jdbc\n"
        + "  name: FoodMart\n"
        + "  jdbcUser: u_baz\n"
        + "  jdbcPassword: p_baz\n"
        + "  jdbcUrl: jdbc:baz\n"
        + "  jdbcCatalog: cat_baz\n"
        + "  jdbcSchema: \n";
  }

  @Override protected String getCustomSchemaWithSubtype() {
    return "---\n"
        + "version: 1.0\n"
        + "schemas: \n"
        + "- type: custom\n"
        + "  name: My Custom Schema\n"
        + "  factory: com.acme.MySchemaFactory\n"
        + "  operand: \n"
        + "    a: foo\n"
        + "    b: \n"
        + "    - 1\n"
        + "    - 3.5\n"
        + "  tables: \n"
        + "  - type: custom\n"
        + "    name: T1\n"
        + "  - type: custom\n"
        + "    name: T2\n"
        + "    operand: {}\n"
        + "  - type: custom\n"
        + "    name: T3\n"
        + "    operand: \n"
        + "      a: foo\n"
        + "- type: custom\n"
        + "  name: has-no-operand\n";
  }

  @Override protected String getImmutableSchemaWithMaterialization() {
    return "---\n"
        + "version: 1.0\n"
        + "defaultSchema: adhoc\n"
        + "schemas: \n"
        + "- name: empty\n"
        + "- name: adhoc\n"
        + "  type: custom\n"
        + "  factory: " + JdbcTest.MySchemaFactory.class.getName() + "\n"
        + "  operand: \n"
        + "    tableName: ELVIS\n"
        + "    mutable: false\n"
        + "  materializations: \n"
        + "  - table: v\n"
        + "    sql: values (1)\n";
  }

  @Override protected String getSchemaWithoutName() {
    return "---\n"
        + "version: 1.0\n"
        + "defaultSchema: adhoc\n"
        + "schemas: [{}]\n";
  }

  @Override protected String getCustomSchemaWithoutFactory() {
    return "---\n"
        + "version: 1.0\n"
        + "defaultSchema: adhoc\n"
        + "schemas: \n"
        + "- type: custom\n"
        + "  name: my_custom_schema\n";
  }

  @Override protected String getLattice() {
    return "---\n"
        + "version: 1.0\n"
        + "schemas: \n"
        + "- name: FoodMart\n"
        + "  tables: \n"
        + "  - name: time_by_day\n"
        + "    columns: \n"
        + "    - name: time_id\n"
        + "  - name: sales_fact_1997\n"
        + "    columns: \n"
        + "    - name: time_id\n"
        + "  - name: V\n"
        + "    type: view\n"
        + "    sql: values (1)\n"
        + "  - name: V2\n"
        + "    type: view\n"
        + "    sql: ['values (1)', '(2)']\n"
        + "  lattices: \n"
        + "  - name: SalesStar\n"
        + "    sql: select * from sales_fact_1997\n"
        + "  - name: SalesStar2\n"
        + "    sql: ['select *', 'from sales_fact_1997']\n";
  }

  @Override protected String getBadMultilineSql() {
    return "---\n"
        + "version: 1.0\n"
        + "schemas: \n"
        + "- name: FoodMart\n"
        + "  tables: \n"
        + "  - name: V\n"
        + "    type: view\n"
        + "    sql: ['values (1)', 2]\n";
  }
}

// End YamlModelTest.java
