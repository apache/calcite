/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.jdbc;

import net.hydromatic.avatica.*;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import java.util.Properties;

/**
 * Extension of {@link net.hydromatic.avatica.AvaticaFactory}
 * for Optiq.
 */
public abstract class OptiqFactory implements AvaticaFactory {
  protected final int major;
  protected final int minor;

  /** Creates a JDBC factory with given major/minor version number. */
  protected OptiqFactory(int major, int minor) {
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
      OptiqSchema rootSchema, JavaTypeFactory typeFactory);
}

// End OptiqFactory.java
