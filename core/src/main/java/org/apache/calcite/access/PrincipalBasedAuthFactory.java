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
package org.apache.calcite.access;

import org.apache.calcite.sql.SqlAccessType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Factory of principal based access
 */
public class PrincipalBasedAuthFactory implements AuthorizationFactory {

  private static final Pattern OWNER_PATTERN = Pattern.compile("\\s*OWNER\\s*");

  @Override public void init(Map<String, String> operand) {
  }

  @Override public Authorization create(Map<String, String> operand) {
    Map<String, SqlAccessType> accessMap = new HashMap<>();
    Set<String> owners = new HashSet<>();
    convert(operand, accessMap, owners);
    return new PrincipalBasedAuthorization(CalcitePrincipalFairy.INSTANCE, accessMap, owners);
  }

  private void convert(
      Map<String, String> operand,
      Map<String, SqlAccessType> accessMap,
      Set<String> owners) {
    for (Map.Entry<String, String> entry : operand.entrySet()) {
      if (OWNER_PATTERN.matcher(entry.getValue()).matches()) {
        owners.add(entry.getKey());
      } else {
        accessMap.put(entry.getKey(), SqlAccessType.create(entry.getValue()));
      }
    }
  }

}

// End PrincipalBasedAuthFactory.java
