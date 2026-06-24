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
package org.apache.calcite.adapter.kvrocks;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.model.JsonCustomTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link KvrocksSchema}, {@link KvrocksTable}, and helpers. */
class KvrocksSchemaTableTest {

  @Test void schemaBuildsTableMapFromModelTables() {
    KvrocksSchema schema =
        schema(table("raw_01", "raw", ":", rawField()),
            table("csv_01", "csv", ":", deptField(), nameField()));

    assertTrue(
        schema.getTableMap().keySet().containsAll(
            ImmutableList.of("raw_01", "csv_01")));
    assertTrue(schema.getTableMap().get("raw_01") instanceof KvrocksTable);
  }

  @Test void schemaExtractsFieldInfo() {
    KvrocksSchema schema =
        schema(table("csv_01", "csv", "|", deptField(), nameField()));

    KvrocksTableFieldInfo info = schema.getTableFieldInfo("csv_01");

    assertEquals("csv_01", info.getTableName());
    assertEquals("csv", info.getDataFormat());
    assertEquals("|", info.getKeyDelimiter());
    assertEquals(2, info.getFields().size());
  }

  @Test void schemaRejectsMissingOperand() {
    KvrocksSchema schema =
        schema(
            new JsonCustomTable("bad", null,
            "org.apache.calcite.adapter.kvrocks.KvrocksTableFactory", null));

    assertThrows(NullPointerException.class,
        () -> schema.getTableFieldInfo("bad"));
  }

  @Test void schemaReturnsEmptyInfoForUnknownTable() {
    KvrocksSchema schema = schema(table("csv_01", "csv", ":", deptField()));

    KvrocksTableFieldInfo info = schema.getTableFieldInfo("missing");

    assertEquals("missing", info.getTableName());
    assertEquals("", info.getDataFormat());
    assertTrue(info.getFields().isEmpty());
  }

  @Test void deduceRowTypeUsesRawKeyColumn() {
    KvrocksTableFieldInfo info = info("raw_01", "raw", rawField());

    Map<String, Object> columns = KvrocksEnumerator.deduceRowType(info);

    assertEquals(1, columns.size());
    assertEquals("key", columns.get("key"));
  }

  @Test void deduceRowTypeUsesConfiguredFields() {
    KvrocksTableFieldInfo info =
        info("csv_01", "csv", deptField(), nameField());

    Map<String, Object> columns = KvrocksEnumerator.deduceRowType(info);

    assertEquals("varchar", columns.get("DEPTNO"));
    assertEquals("varchar", columns.get("NAME"));
  }

  @Test void tableDerivesRowTypeFromFields() {
    KvrocksSchema schema =
        schema(table("csv_01", "csv", ":", deptField(), nameField()));
    KvrocksTable table =
        (KvrocksTable) KvrocksTable.create(schema, "csv_01",
            new KvrocksConfig("localhost", 6666, 0, ""), null);

    RelDataType rowType = table.getRowType(new JavaTypeFactoryImpl());

    assertEquals(2, rowType.getFieldCount());
    assertEquals("DEPTNO", rowType.getFieldList().get(0).getName());
    assertEquals("NAME", rowType.getFieldList().get(1).getName());
  }

  @Test void tableUsesProtoRowTypeWhenProvided() {
    KvrocksSchema schema =
        schema(table("csv_01", "csv", ":", deptField(), nameField()));
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType protoType = typeFactory.builder()
        .add("ID", typeFactory.createJavaType(Integer.class))
        .build();
    KvrocksTable table =
        (KvrocksTable) KvrocksTable.create(schema, "csv_01",
            new KvrocksConfig("localhost", 6666, 0, ""),
            factory -> protoType);

    RelDataType rowType = table.getRowType(typeFactory);

    assertEquals(1, rowType.getFieldCount());
    assertEquals("ID", rowType.getFieldList().get(0).getName());
  }

  @Test void tableScanCreatesEnumerable() {
    KvrocksSchema schema = schema(table("raw_01", "raw", ":", rawField()));
    KvrocksTable table =
        (KvrocksTable) KvrocksTable.create(schema, "raw_01",
            new KvrocksConfig("localhost", 6666, 0, ""), null);

    assertNotNull(table.scan(null));
  }

  @Test void tableScanEnumeratorReadsRowsThroughManager() {
    KvrocksTableFieldInfo info = info("raw_01", "raw", rawField());
    KvrocksJedisManager manager = Mockito.mock(KvrocksJedisManager.class);
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(manager.getResource()).thenReturn(jedis);
    Mockito.when(jedis.type("raw_01")).thenReturn("string");
    Mockito.when(jedis.keys("raw_01"))
        .thenReturn(java.util.Collections.singleton("raw_01"));
    Mockito.when(jedis.get("raw_01")).thenReturn("hello");
    KvrocksSchema schema = new TestKvrocksSchema(info, manager);
    KvrocksTable table =
        (KvrocksTable) KvrocksTable.create(schema, "raw_01",
            new KvrocksConfig("localhost", 6666, 0, "secret"), null);

    org.apache.calcite.linq4j.Enumerator<Object[]> enumerator =
        table.scan(null).enumerator();

    assertTrue(enumerator.moveNext());
    assertEquals("hello", enumerator.current()[0]);
    enumerator.close();
    Mockito.verify(jedis).auth("secret");
    Mockito.verify(jedis).close();
  }

  @Test void tableScanAuthenticatesWithNamespaceToken() {
    KvrocksTableFieldInfo info = info("raw_01", "raw", rawField());
    KvrocksJedisManager manager = Mockito.mock(KvrocksJedisManager.class);
    Jedis jedis = Mockito.mock(Jedis.class);
    Mockito.when(manager.getResource()).thenReturn(jedis);
    Mockito.when(jedis.type("raw_01")).thenReturn("string");
    Mockito.when(jedis.keys("raw_01"))
        .thenReturn(java.util.Collections.singleton("raw_01"));
    Mockito.when(jedis.get("raw_01")).thenReturn("hello");
    KvrocksSchema schema = new TestKvrocksSchema(info, manager);
    KvrocksTable table =
        (KvrocksTable) KvrocksTable.create(schema, "raw_01",
            new KvrocksConfig("localhost", 6666, 0,
                "administrator-token", "namespace-token"), null);

    org.apache.calcite.linq4j.Enumerator<Object[]> enumerator =
        table.scan(null).enumerator();

    assertTrue(enumerator.moveNext());
    enumerator.close();
    Mockito.verify(jedis).auth("namespace-token");
    Mockito.verify(jedis, Mockito.never()).auth("administrator-token");
  }

  @Test void tableFactoryCreatesKvrocksTable() {
    KvrocksSchema kvrocksSchema = schema(table("raw_01", "raw", ":", rawField()));
    SchemaPlus schemaPlus = Mockito.mock(SchemaPlus.class);
    Mockito.when(schemaPlus.unwrap(KvrocksSchema.class)).thenReturn(kvrocksSchema);

    Table table =
        KvrocksTableFactory.INSTANCE.create(schemaPlus, "raw_01",
            java.util.Collections.emptyMap(), null);

    assertTrue(table instanceof KvrocksTable);
  }

  @Test void schemaFactoryCreatesKvrocksSchema() {
    Map<String, Object> operand = new java.util.HashMap<>();
    operand.put("host", "localhost");
    operand.put("port", 6666);
    operand.put("database", "0");
    operand.put("tables", java.util.Collections.emptyList());

    Schema schema = new KvrocksSchemaFactory()
        .create(Mockito.mock(SchemaPlus.class), "kvrocks", operand);

    assertTrue(schema instanceof KvrocksSchema);
    KvrocksSchema kvrocksSchema = (KvrocksSchema) schema;
    assertEquals("localhost", kvrocksSchema.host);
    assertEquals(6666, kvrocksSchema.port);
    assertEquals(0, kvrocksSchema.database);
  }

  @Test void jedisManagerCreatesPoolAndCloses() {
    KvrocksJedisManager manager =
        new KvrocksJedisManager("localhost", 6666, 0, "");

    assertNotNull(manager.getJedisPool());
    manager.close();
  }

  private static KvrocksSchema schema(JsonCustomTable... tables) {
    @SuppressWarnings({"rawtypes", "unchecked"})
    List<Map<String, Object>> tableList = (List) Arrays.asList(tables);
    return new KvrocksSchema("localhost", 6666, 0, "",
        tableList);
  }

  private static JsonCustomTable table(String name, String format,
      String delimiter, LinkedHashMap<String, Object>... fields) {
    LinkedHashMap<String, Object> operand = new LinkedHashMap<>();
    operand.put("dataFormat", format);
    operand.put("keyDelimiter", delimiter);
    operand.put("fields", Arrays.asList(fields));
    return new JsonCustomTable(name, null,
        "org.apache.calcite.adapter.kvrocks.KvrocksTableFactory", operand);
  }

  private static KvrocksTableFieldInfo info(String tableName, String format,
      LinkedHashMap<String, Object>... fields) {
    KvrocksTableFieldInfo info = new KvrocksTableFieldInfo();
    info.setTableName(tableName);
    info.setDataFormat(format);
    info.setFields(Arrays.asList(fields));
    return info;
  }

  private static LinkedHashMap<String, Object> rawField() {
    LinkedHashMap<String, Object> field = new LinkedHashMap<>();
    field.put("name", "key");
    field.put("type", "varchar");
    field.put("mapping", "key");
    return field;
  }

  private static LinkedHashMap<String, Object> deptField() {
    LinkedHashMap<String, Object> field = new LinkedHashMap<>();
    field.put("name", "DEPTNO");
    field.put("type", "varchar");
    field.put("mapping", "DEPTNO");
    return field;
  }

  private static LinkedHashMap<String, Object> nameField() {
    LinkedHashMap<String, Object> field = new LinkedHashMap<>();
    field.put("name", "NAME");
    field.put("type", "varchar");
    field.put("mapping", "NAME");
    return field;
  }

  /** Schema that injects mocked table metadata and connection manager. */
  private static class TestKvrocksSchema extends KvrocksSchema {
    private final KvrocksTableFieldInfo fieldInfo;
    private final KvrocksJedisManager manager;

    TestKvrocksSchema(KvrocksTableFieldInfo fieldInfo,
        KvrocksJedisManager manager) {
      super("localhost", 6666, 0, "",
          java.util.Collections.emptyList());
      this.fieldInfo = fieldInfo;
      this.manager = manager;
    }

    @Override KvrocksJedisManager getManager() {
      return manager;
    }

    @Override KvrocksTableFieldInfo getTableFieldInfo(String tableName) {
      assertEquals(fieldInfo.getTableName(), tableName);
      return fieldInfo;
    }
  }
}
