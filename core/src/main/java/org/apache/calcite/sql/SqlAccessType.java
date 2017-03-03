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
package org.apache.calcite.sql;

import java.util.EnumSet;
import java.util.Locale;

/**
 * SqlAccessType is represented by a set of allowed access types
 */
public class SqlAccessType {
  //~ Static fields/initializers ---------------------------------------------

  public static final SqlAccessType ALL =
      new SqlAccessType(EnumSet.allOf(SqlAccessEnum.class));
  public static final SqlAccessType READ_ONLY =
      new SqlAccessType(EnumSet.of(SqlAccessEnum.SELECT));
  public static final SqlAccessType WRITE_ONLY =
      new SqlAccessType(EnumSet.of(SqlAccessEnum.INSERT));

  //~ Instance fields --------------------------------------------------------

  private final EnumSet<SqlAccessEnum> accessEnums;

  //~ Constructors -----------------------------------------------------------

  public SqlAccessType(EnumSet<SqlAccessEnum> accessEnums) {
    this.accessEnums = accessEnums;
  }

  //~ Methods ----------------------------------------------------------------

  public boolean allowsAccess(SqlAccessEnum access) {
    return accessEnums.contains(access);
  }

  public String toString() {
    return accessEnums.toString();
  }

  public static SqlAccessType create(String[] accessNames) {
    assert accessNames != null;
    EnumSet<SqlAccessEnum> enumSet = EnumSet.noneOf(SqlAccessEnum.class);
    for (String accessName : accessNames) {
      enumSet.add(
          SqlAccessEnum.valueOf(accessName.trim().toUpperCase(Locale.ROOT)));
    }
    return new SqlAccessType(enumSet);
  }

  public static SqlAccessType create(String accessString) {
    assert accessString != null;
    accessString = accessString.replace('[', ' ');
    accessString = accessString.replace(']', ' ');
    String[] accessNames = accessString.split(",");
    return create(accessNames);
  }
}

// End SqlAccessType.java
