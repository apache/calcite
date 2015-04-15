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
package org.apache.calcite.server;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;

import java.util.Iterator;

/**
 * Statement within a Calcite server.
 */
public interface CalciteServerStatement {
  /** Creates a context for preparing a statement for execution. */
  CalcitePrepare.Context createPrepareContext();

  /** Returns the connection. */
  CalciteConnection getConnection();

  void setSignature(Meta.Signature signature);

  Meta.Signature getSignature();

  Iterator<Object> getResultSet();

  void setResultSet(Iterator<Object> resultSet);
}

// End CalciteServerStatement.java
