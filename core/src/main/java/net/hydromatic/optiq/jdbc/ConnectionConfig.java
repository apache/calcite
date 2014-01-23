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

import net.hydromatic.avatica.Casing;
import net.hydromatic.avatica.Quoting;

/** Interface for reading connection properties within Optiq code. There is
 * a method for every property. At some point there will be similar config
 * classes for system and statement properties. */
public interface ConnectionConfig {
  boolean autoTemp();
  boolean materializationsEnabled();
  String model();
  String schema();
  Lex lex();
  Quoting quoting();
  Casing unquotedCasing();
  Casing quotedCasing();
  boolean caseSensitive();
  boolean spark();

  enum Lex {
    ORACLE(Quoting.DOUBLE_QUOTE, Casing.TO_UPPER, Casing.UNCHANGED, true),
    MYSQL(Quoting.BACK_TICK, Casing.UNCHANGED, Casing.UNCHANGED, false),
    SQL_SERVER(Quoting.BRACKET, Casing.UNCHANGED, Casing.UNCHANGED, false),
    JAVA(Quoting.BACK_TICK, Casing.UNCHANGED, Casing.UNCHANGED, true);

    public final Quoting quoting;
    public final Casing unquotedCasing;
    public final Casing quotedCasing;
    public final boolean caseSensitive;

    Lex(Quoting quoting, Casing unquotedCasing, Casing quotedCasing,
        boolean caseSensitive) {
      this.quoting = quoting;
      this.unquotedCasing = unquotedCasing;
      this.quotedCasing = quotedCasing;
      this.caseSensitive = caseSensitive;
    }
  }
}

// End ConnectionConfig.java
