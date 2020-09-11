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

package org.apache.calcite.adapter.arrow;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Source;

import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ArrowTest {

  /**
   * Test to read Arrow file and check it's field name and type
   */
  @Test void testArrowSchema() {
    Source source = Sources.of(ArrowTest.class.getResource("/files"));
    ArrowSchema arrowSchema = new ArrowSchema(source.file().getAbsoluteFile());
    Map<String, Table> tableMap = arrowSchema.getTableMap();
    RelDataType relDataType = tableMap.get("TEST").getRowType(new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
    Assertions.assertEquals(relDataType.getFieldNames().get(0), "column1");
    Assertions.assertEquals(relDataType.getFieldList().get(0).getType().toString(), "INTEGER");
  }
}
