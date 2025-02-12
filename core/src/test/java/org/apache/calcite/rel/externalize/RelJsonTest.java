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
package org.apache.calcite.rel.externalize;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.DiffRepository;
import org.apache.calcite.util.JsonBuilder;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for @{@link RelJson}.
 */
public class RelJsonTest {

  private static final DiffRepository REPO =  DiffRepository.lookup(RelJsonTest.class);

  @Test void testToJsonWithStructRelDatatypeField() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = typeFactory.builder()
        .add("street", SqlTypeName.VARCHAR, 50)
        .add("number", SqlTypeName.INTEGER)
        .add("building", SqlTypeName.VARCHAR, 20).nullable(true)
        .build();
    RelDataTypeField address =
        new RelDataTypeFieldImpl("address", 0, type);

    JsonBuilder builder = new JsonBuilder();
    Object jsonObj = RelJson.create().withJsonBuilder(builder).toJson(address);
    REPO.assertEquals("content", "${content}", builder.toJsonString(jsonObj));
  }

}
