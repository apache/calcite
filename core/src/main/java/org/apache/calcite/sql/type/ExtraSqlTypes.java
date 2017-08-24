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
package org.apache.calcite.sql.type;

/**
 * Holds constants associated with SQL types introduced after the earliest
 * version of Java supported by Farrago (this currently means anything
 * introduced in JDK 1.6 or later).
 *
 * <p>Allows us to deal sanely with type constants returned by newer JDBC
 * drivers when running a version of Farrago compiled under an old
 * version of the JDK (i.e. 1.5).
 *
 * <p>By itself, the presence of a constant here doesn't imply that farrago
 * fully supports the associated type.  This is simply a mirror of the
 * missing constant values.
 */
public interface ExtraSqlTypes {
  // From JDK 1.6
  int ROWID = -8;
  int NCHAR = -15;
  int NVARCHAR = -9;
  int LONGNVARCHAR = -16;
  int NCLOB = 2011;
  int SQLXML = 2009;

  // From JDK 1.8
  int REF_CURSOR = 2012;
  int TIME_WITH_TIMEZONE = 2013;
  int TIMESTAMP_WITH_TIMEZONE = 2014;

  // From OpenGIS
  int GEOMETRY = 2015; // TODO: confirm
}

// End ExtraSqlTypes.java
