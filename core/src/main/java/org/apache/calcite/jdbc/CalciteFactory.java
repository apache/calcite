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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.UnregisteredDriver;

import java.util.Properties;

/**
 * Extension of {@link org.apache.calcite.avatica.AvaticaFactory}
 * for Calcite.
 */
public abstract class CalciteFactory implements AvaticaFactory {
  protected final int major;
  protected final int minor;

  /** Creates a JDBC factory with given major/minor version number. */
  protected CalciteFactory(int major, int minor) {
    this.major = major;
    this.minor = minor;
  }

  public int getJdbcMajorVersion() {
    return major;
  }

  public int getJdbcMinorVersion() {
    return minor;
  }

  public final AvaticaConnection newConnection(
      UnregisteredDriver driver,
      AvaticaFactory factory,
      String url,
      Properties info) {
    return newConnection(driver, factory, url, info, null, null);
  }

  /** Creates a connection with a root schema. */
  public abstract AvaticaConnection newConnection(UnregisteredDriver driver,
      AvaticaFactory factory, String url, Properties info,
      CalciteSchema rootSchema, JavaTypeFactory typeFactory);
}

// End CalciteFactory.java
