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
package org.apache.calcite.adapter.file.categories;

/**
 * Marker interface for performance tests.
 *
 * <p>Performance tests should:
 * <ul>
 *   <li>Measure execution time, memory usage, or throughput</li>
 *   <li>Take longer to run (typically several seconds or minutes)</li>
 *   <li>Generate performance reports or benchmarks</li>
 *   <li>Be excluded from standard test runs</li>
 *   <li>Only run when explicitly requested</li>
 * </ul>
 *
 * <p>Performance tests are typically disabled by default and enabled
 * via system properties or profiles:
 * <pre>{@code
 * @Category(PerformanceTest.class)
 * public class MyPerformanceTest {
 *   @BeforeEach
 *   void checkEnabled() {
 *     assumeTrue(Boolean.getBoolean("enablePerformanceTests"),
 *         "Performance tests disabled");
 *   }
 * }
 * }</pre>
 */
public interface PerformanceTest {
  // Marker interface - no methods needed
}
