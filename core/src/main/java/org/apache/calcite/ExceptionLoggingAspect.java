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
  public static final String METHOD_CALL_STACK = "methodCall";
  public static final String SQL_EXPRESSION = "sqlExpression";
  public static final String REL_EXPRESSION = "relExpression";
  public static final String ROOT_METHOD_SIGNATURE = "rootMethodSignature";
  public static final String ROOT_METHOD_ARGS = "rootMethodArgs";
  public static final String FAILING_MODULE = "failingModule";
  public static final String FAILURE_STAGE = "failureStage";
  public static final String SOURCE_SCRIPT = "sourceScript";
  public static final String EXCEPTION_MSG = "exceptionMsg";


  public Map<String, List<String>> details;

  public ExceptionLoggingAspect() {
    details = new HashMap<>();
    details.put(REL_NODE, new ArrayList<String>());
    details.put(METHOD_CALL_STACK, new ArrayList<String>());
    details.put(SQL_EXPRESSION, new ArrayList<String>());
    details.put(REL_EXPRESSION, new ArrayList<String>());
    details.put(ROOT_METHOD_SIGNATURE, new ArrayList<String>());
    details.put(ROOT_METHOD_ARGS, new ArrayList<String>());
    details.put(FAILING_MODULE, new ArrayList<String>());
    details.put(FAILURE_STAGE, new ArrayList<String>());
    details.put(SOURCE_SCRIPT, new ArrayList<String>());
    details.put(EXCEPTION_MSG, new ArrayList<String>());
  }

  // Getting populated from calcite
  public List<String> getRelNodeExceptionDetails() {
    return details.get(REL_NODE);
  }

  // Getting populated from calcite as well as raven
  public List<String> getMethodCallExceptionDetails() {
    return details.get(METHOD_CALL_STACK);
  }

  // Getting populated from raven
  public List<String> getSqlExpressions() {
    return details.get(SQL_EXPRESSION);
  }

  // Getting populated from calcite as well as raven
  public List<String> getRelExpressions() {
    return details.get(REL_EXPRESSION);
  }

  // Getting populated from raven
  public List<String> getRootMethodSignature() {
    return details.get(ROOT_METHOD_SIGNATURE);
  }

  // Getting populated from raven
  public List<String> getRootMethodArgs() {
    return details.get(ROOT_METHOD_ARGS);
  }

  // Getting populated from raven
  public List<String> getFailingModule() {
    return details.get(FAILING_MODULE);
  }

  // Getting populated from raven
  public List<String> getFailureStage() {
    return details.get(FAILURE_STAGE);
  }

  // Getting populated from raven
  public List<String> getSourceScript() {
    return details.get(SOURCE_SCRIPT);
  }

  // Getting populated from raven
  public List<String> getExceptionMsg() {
    return details.get(EXCEPTION_MSG);
  }

  public void clear() {
    details.forEach((key, value) -> value.clear());
    details.clear();
  }

}
