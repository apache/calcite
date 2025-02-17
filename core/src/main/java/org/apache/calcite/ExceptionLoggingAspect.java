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
package org.apache.calcite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class use by aspect implementation to segregate relNode and method call detail which
 * captured during aspect execution.
 */
public class ExceptionLoggingAspect extends RuntimeException {

  public static final String REL_NODE = "relNode";
  public static final String METHOD_CALL = "methodCall";
  public static final String SQL_EXPRESSION = "sqlExpression";
  public static final String REL_EXPRESSION = "relExpression";

  public Map<String, List<String>> details;

  public ExceptionLoggingAspect() {
    details = new HashMap<>();
    details.put(REL_NODE, new ArrayList<String>());
    details.put(METHOD_CALL, new ArrayList<String>());
    details.put(SQL_EXPRESSION, new ArrayList<String>());
    details.put(REL_EXPRESSION, new ArrayList<String>());
  }

  // Getting populated from calcite
  public List<String> getRelNodeExceptionDetails() {
    return details.get(REL_NODE);
  }

  // Getting populated from calcite as well as raven
  public List<String> getMethodCallExceptionDetails() {
    return details.get(METHOD_CALL);
  }

  // Getting populated from raven
  public List<String> getSqlExpressions() {
    return details.get(SQL_EXPRESSION);
  }

  // Getting populated from calcite as well as raven
  public List<String> getRelExpressions() {
    return details.get(REL_EXPRESSION);
  }

  public void clear() {
    details.forEach((key, value) -> value.clear());
    details.clear();
  }
}
