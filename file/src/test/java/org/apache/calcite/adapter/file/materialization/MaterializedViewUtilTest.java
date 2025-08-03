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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test for MaterializedViewUtil.
 */
public class MaterializedViewUtilTest {

  @Test public void testEngineSpecificExtensions() {
    // Test default CSV extension
    assertThat(MaterializedViewUtil.getFileExtension("LINQ4J"), is("csv"));
    assertThat(MaterializedViewUtil.getFileExtension("VECTORIZED"), is("csv"));
    assertThat(MaterializedViewUtil.getFileExtension(null), is("csv"));

    // Test Parquet extension
    assertThat(MaterializedViewUtil.getFileExtension("PARQUET"), is("parquet"));
    assertThat(MaterializedViewUtil.getFileExtension("parquet"), is("parquet"));

    // Test Arrow extension
    assertThat(MaterializedViewUtil.getFileExtension("ARROW"), is("arrow"));
    assertThat(MaterializedViewUtil.getFileExtension("arrow"), is("arrow"));
  }

  @Test public void testMaterializedViewFilename() {
    ExecutionEngineConfig csvConfig = new ExecutionEngineConfig("LINQ4J", 1000);
    assertThat(MaterializedViewUtil.getMaterializedViewFilename("sales_summary", csvConfig),
        is("sales_summary.csv"));

    ExecutionEngineConfig parquetConfig = new ExecutionEngineConfig("PARQUET", 10000);
    assertThat(MaterializedViewUtil.getMaterializedViewFilename("sales_summary", parquetConfig),
        is("sales_summary.parquet"));

    ExecutionEngineConfig arrowConfig = new ExecutionEngineConfig("ARROW", 5000);
    assertThat(MaterializedViewUtil.getMaterializedViewFilename("product_stats", arrowConfig),
        is("product_stats.arrow"));
  }

  @Test public void testIsMaterializedViewFile() {
    // CSV files for LINQ4J and VECTORIZED
    assertThat(MaterializedViewUtil.isMaterializedViewFile("sales.csv", "LINQ4J"), is(true));
    assertThat(MaterializedViewUtil.isMaterializedViewFile("sales.csv", "VECTORIZED"), is(true));
    assertThat(MaterializedViewUtil.isMaterializedViewFile("sales.parquet", "LINQ4J"), is(false));

    // Parquet files for PARQUET engine
    assertThat(MaterializedViewUtil.isMaterializedViewFile("sales.parquet", "PARQUET"), is(true));
    assertThat(MaterializedViewUtil.isMaterializedViewFile("sales.csv", "PARQUET"), is(false));

    // Arrow files for ARROW engine
    assertThat(MaterializedViewUtil.isMaterializedViewFile("sales.arrow", "ARROW"), is(true));
    assertThat(MaterializedViewUtil.isMaterializedViewFile("sales.csv", "ARROW"), is(false));
  }
}
