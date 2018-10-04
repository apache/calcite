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

import org.apache.calcite.adapter.os.OsAdapterTest;
import org.apache.calcite.adapter.tpcds.TpcdsTest;
import org.apache.calcite.adapter.tpch.TpchTest;
import org.apache.calcite.chinook.EndToEndTest;
import org.apache.calcite.chinook.RemotePreparedStatementParametersTest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Suite consisting of all tests in the <code>calcite-plus</code> module.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    OsAdapterTest.class,
    TpcdsTest.class,
    TpchTest.class,
    EndToEndTest.class,
    RemotePreparedStatementParametersTest.class
    })
public class PlusSuite {
}

// End PlusSuite.java
