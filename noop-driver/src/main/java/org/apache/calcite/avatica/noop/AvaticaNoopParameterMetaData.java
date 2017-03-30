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
package org.apache.calcite.avatica.noop;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

/**
 * An implementation of {@link ParameterMetaData} which does nothing.
 */
public class AvaticaNoopParameterMetaData implements ParameterMetaData {

  private static final AvaticaNoopParameterMetaData INSTANCE = new AvaticaNoopParameterMetaData();

  public static AvaticaNoopParameterMetaData getInstance() {
    return INSTANCE;
  }

  private AvaticaNoopParameterMetaData() {}

  private UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("Unsupported");
  }

  @Override public <T> T unwrap(Class<T> iface) throws SQLException {
    throw unsupported();
  }

  @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  @Override public int getParameterCount() throws SQLException {
    return 0;
  }

  @Override public int isNullable(int param) throws SQLException {
    return 0;
  }

  @Override public boolean isSigned(int param) throws SQLException {
    return false;
  }

  @Override public int getPrecision(int param) throws SQLException {
    return 0;
  }

  @Override public int getScale(int param) throws SQLException {
    return 0;
  }

  @Override public int getParameterType(int param) throws SQLException {
    return 0;
  }

  @Override public String getParameterTypeName(int param) throws SQLException {
    throw unsupported();
  }

  @Override public String getParameterClassName(int param) throws SQLException {
    return "java.lang.Object";
  }

  @Override public int getParameterMode(int param) throws SQLException {
    return 0;
  }

}

// End AvaticaNoopParameterMetaData.java
