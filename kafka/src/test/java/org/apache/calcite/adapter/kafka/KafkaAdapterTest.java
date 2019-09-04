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
package org.apache.calcite.adapter.kafka;

import org.apache.calcite.test.CalciteAssert;

import com.google.common.io.Resources;

import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Unit test cases for Kafka adapter.
 */
public class KafkaAdapterTest {
  protected static final URL MODEL = KafkaAdapterTest.class.getResource("/kafka.model.json");

  private CalciteAssert.AssertThat assertModel(String model) {
    // ensure that Schema from this instance is being used
    model = model.replace(KafkaAdapterTest.class.getName(), KafkaAdapterTest.class.getName());

    return CalciteAssert.that()
        .withModel(model);
  }

  private CalciteAssert.AssertThat assertModel(URL url) {
    Objects.requireNonNull(url, "url");
    try {
      return assertModel(Resources.toString(url, StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test public void testSelect() {
    assertModel(MODEL)
        .query("SELECT STREAM * FROM KAFKA.MOCKTABLE")
        .limit(2)

        .typeIs("[MSG_PARTITION INTEGER NOT NULL"
            + ", MSG_TIMESTAMP BIGINT NOT NULL"
            + ", MSG_OFFSET BIGINT NOT NULL"
            + ", MSG_KEY_BYTES VARBINARY"
            + ", MSG_VALUE_BYTES VARBINARY NOT NULL]")

        .returnsUnordered(
            "MSG_PARTITION=0; MSG_TIMESTAMP=-1; MSG_OFFSET=0; MSG_KEY_BYTES=mykey0; MSG_VALUE_BYTES=myvalue0",
            "MSG_PARTITION=0; MSG_TIMESTAMP=-1; MSG_OFFSET=1"
                + "; MSG_KEY_BYTES=mykey1; MSG_VALUE_BYTES=myvalue1")

        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  BindableTableScan(table=[[KAFKA, MOCKTABLE, (STREAM)]])\n");
  }

  @Test public void testFilterWithProject() {
    assertModel(MODEL)
        .query("SELECT STREAM MSG_PARTITION,MSG_OFFSET,MSG_VALUE_BYTES FROM KAFKA.MOCKTABLE"
            + " WHERE MSG_OFFSET>0")
        .limit(1)

        .returnsUnordered(
            "MSG_PARTITION=0; MSG_OFFSET=1; MSG_VALUE_BYTES=myvalue1")
        .explainContains(
            "PLAN=EnumerableCalc(expr#0..4=[{inputs}], expr#5=[0], expr#6=[>($t2, $t5)], MSG_PARTITION=[$t0], MSG_OFFSET=[$t2], MSG_VALUE_BYTES=[$t4], $condition=[$t6])\n"
                + "  EnumerableInterpreter\n"
                + "    BindableTableScan(table=[[KAFKA, MOCKTABLE, (STREAM)]])");
  }

  @Test public void testCustRowConverter() {
    assertModel(MODEL)
        .query("SELECT STREAM * FROM KAFKA.MOCKTABLE_CUST_ROW_CONVERTER")
        .limit(2)

        .typeIs("[TOPIC_NAME VARCHAR NOT NULL"
            + ", PARTITION_ID INTEGER NOT NULL"
            + ", TIMESTAMP_TYPE VARCHAR]")

        .returnsUnordered(
            "TOPIC_NAME=testtopic; PARTITION_ID=0; TIMESTAMP_TYPE=NoTimestampType",
            "TOPIC_NAME=testtopic; PARTITION_ID=0; TIMESTAMP_TYPE=NoTimestampType")

        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  BindableTableScan(table=[[KAFKA, MOCKTABLE_CUST_ROW_CONVERTER, (STREAM)]])\n");
  }


  @Test public void testAsBatch() {
    assertModel(MODEL)
        .query("SELECT * FROM KAFKA.MOCKTABLE")
        .failsAtValidation("Cannot convert stream 'MOCKTABLE' to relation");
  }
}

// End KafkaAdapterTest.java
