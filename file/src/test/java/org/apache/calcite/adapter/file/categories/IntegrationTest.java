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
 * Marker interface for integration tests.
 * 
 * <p>Integration tests should:
 * <ul>
 *   <li>Test interactions between multiple components</li>
 *   <li>May require external services (Redis, SharePoint, FTP, etc.)</li>
 *   <li>Should be conditional on credentials/connection properties</li>
 *   <li>Take longer to run than unit tests</li>
 *   <li>Use real external dependencies when available</li>
 * </ul>
 * 
 * <p>Integration tests must check for required credentials/properties
 * and skip themselves if not available:
 * <pre>{@code
 * @Category(IntegrationTest.class)
 * public class MyIntegrationTest {
 *   @BeforeEach
 *   void checkCredentials() {
 *     assumeTrue(hasRequiredCredentials(), "Skipping - credentials not available");
 *   }
 * }
 * }</pre>
 */
public interface IntegrationTest {
  // Marker interface - no methods needed
}