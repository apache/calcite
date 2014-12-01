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
package org.apache.calcite.jdbc;

/**
 * Implementation of {@link org.apache.calcite.avatica.AvaticaFactory}
 * for Calcite and JDBC 4.0 (corresponds to JDK 1.6).
 */
public class CalciteJdbc40Factory extends CalciteJdbc41Factory {
  /** Creates a factory for JDBC version 4.1. */
  public CalciteJdbc40Factory() {
    super(4, 0);
  }
}

// End CalciteJdbc40Factory.java
