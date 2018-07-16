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

import org.apache.calcite.adapter.mongodb.MongoAdapterTest;

import org.junit.BeforeClass;

import static org.junit.Assume.assumeTrue;

/**
 * Used to trigger integration tests from maven (thus class name is suffixed with {@code IT}).
 *
 * <p>If you want to run integration tests from the, IDE manually set the
 * {@code -Dcalcite.integrationTest=true} system property.
 *
 * <p>For command line use:
 * <pre>
 *     $ mvn install -Pit
 * </pre>
 */
public class MongoAdapterIT extends MongoAdapterTest {

  @BeforeClass
  public static void enforceMongo() {
    assumeTrue(MongoAssertions.useMongo());
  }
}

// End MongoAdapterIT.java
