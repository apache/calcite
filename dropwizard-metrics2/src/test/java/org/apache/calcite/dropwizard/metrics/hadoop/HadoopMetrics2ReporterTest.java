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
package org.apache.calcite.dropwizard.metrics.hadoop;

import org.apache.calcite.dropwizard.metrics.hadoop.HadoopMetrics2Reporter.Builder;

import org.apache.hadoop.metrics2.MetricsSystem;

import com.codahale.metrics.MetricRegistry;

import org.junit.Before;
import org.junit.Test;

import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link HadoopMetrics2Reporter}.
 */
public class HadoopMetrics2ReporterTest {

  private MetricRegistry mockRegistry;
  private MetricsSystem mockMetricsSystem;

  @Before public void setup() {
    mockRegistry = Mockito.mock(MetricRegistry.class);
    mockMetricsSystem = Mockito.mock(MetricsSystem.class);
  }

  @Test public void testBuilderDefaults() {
    Builder builder = HadoopMetrics2Reporter.forRegistry(mockRegistry);

    final String jmxContext = "MyJmxContext;sub=Foo";
    final String desc = "Description";
    final String recordName = "Metrics";

    HadoopMetrics2Reporter reporter =
        builder.build(mockMetricsSystem, jmxContext, desc, recordName);

    assertEquals(mockMetricsSystem, reporter.getMetrics2System());
    // The Context "tag", not the jmx context
    assertEquals(null, reporter.getContext());
    assertEquals(recordName, reporter.getRecordName());
  }

}

// End HadoopMetrics2ReporterTest.java
