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

import org.apache.calcite.piglet.PigConverter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.FrameworkConfig;

import org.junit.jupiter.api.BeforeEach;

import static org.apache.calcite.piglet.PigConverter.create;
import static org.apache.calcite.test.PigRelBuilderTest.config;

import static org.junit.jupiter.api.Assumptions.assumeFalse;

import static java.lang.System.getProperty;

/**
 * Abstract class for Pig to {@link RelNode} tests.
 *
 * <p>Under JDK 23 and higher, this test requires
 * "{@code -Djava.security.manager=allow}" command-line arguments due to
 * Hadoop's use of deprecated methods in {@link javax.security.auth.Subject}.
 * These arguments are set automatically if you run via Gradle.
 */
public abstract class PigRelTestBase {
  PigConverter converter;

  @BeforeEach
  public void testSetup() throws Exception {
    assumeFalse(getProperty("os.name").startsWith("Windows"),
        "Skip: Pig/Hadoop tests do not work on Windows");

    final FrameworkConfig config = config().build();
    converter = create(config);
  }
}
