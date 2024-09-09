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

/*

package org.apache.calcite;

*/

/**
 * Define aspect for calcite. It will help to identify problematic code
 * easily and quickly
 *//*

@Aspect
public class LogCalciteException {

  private static final String BASE_PACKAGE = "org.apache.calcite";
  private static final String REL_PACKAGE = "org.apache.calcite.rel..*";
  private static final String TOOL_PACKAGE = "org.apache.calcite.tool..*";
  public static final String MESSAGE_PREFIX = "The issue occurred because of ";
  public static final int METHOD_CALL_LIST_LIMIT = 5;

  @AfterThrowing(pointcut =
      "execution(* " + BASE_PACKAGE + "..*(..)) && (within(" + REL_PACKAGE + ") || "
          + "within(" + TOOL_PACKAGE + "))", throwing = "ex")
  public void logException(JoinPoint joinPoint, Exception ex) {
    if (joinPoint.getTarget() != null) {
      // Capture detail as part of suppressed exception and keep populating it
      // to newly create or thrown exception instance
      ExceptionLoggingAspect loggingException = ex.getSuppressed() != null && ex.getSuppressed().length > 0
              ? (ExceptionLoggingAspect) ex.getSuppressed()[0] : new ExceptionLoggingAspect();
      RelNode relNode = getRelNode(joinPoint);
      // Check whether relNode not null and related to relnode details not populated.
      if (relNode != null && loggingException.getRelNodeExceptionDetails().isEmpty()) {
        loggingException.getRelNodeExceptionDetails().add(MESSAGE_PREFIX + relNode.explain());
        populateSuppressedException(ex, loggingException);
      } else {
        // We populate relNode detail which is being passed as parameter while calling a method
        // also captured details of other kind of parameter with certain threshold limit.
        List<String> relExpressionList = loggingException.getRelExpressions();
        boolean relExpressionListEmpty = relExpressionList.isEmpty();
        String detail = MESSAGE_PREFIX + "method call " + joinPoint.getSignature().toString()
                + " with args ";
        for (Object arg : joinPoint.getArgs()) {
          detail += arg.toString() + ",";
          if (relExpressionListEmpty) {
            populateRelExpressionList(relExpressionList, arg);
            relExpressionListEmpty = relExpressionList.isEmpty();
          }
        }
        if (loggingException.getMethodCallExceptionDetails().size() < METHOD_CALL_LIST_LIMIT) {
          detail = detail.substring(0, detail.length() - 1) + "\n";
          loggingException.getMethodCallExceptionDetails().add(MESSAGE_PREFIX + detail);
        }
        populateSuppressedException(ex, loggingException);
      }
    }
  }

  // If exception is thrown from RelBuilder, SingleRel or sub class of SingleRel
  // then we try to get relnode at that time.
  private RelNode getRelNode(JoinPoint joinPoint) {
    if (joinPoint.getThis() instanceof RelBuilder
            && ((RelBuilder) joinPoint.getThis()).size() > 0) {
      return ((RelBuilder) joinPoint.getThis()).peek();
    } else if (joinPoint.getThis() instanceof SingleRel
            && ((SingleRel) joinPoint.getThis()).getInput() != null) {
      return ((SingleRel) joinPoint.getThis()).getInput();
    }
    return null;
  }

  public static void populateSuppressedException(Exception ex, ExceptionLoggingAspect loggingException) {
    if (ex.getSuppressed() == null || ex.getSuppressed().length == 0) {
      ex.addSuppressed(loggingException);
    }
  }

  private void populateRelExpressionList(List<String> expressionList, Object arg) {
    if (arg instanceof AbstractRelNode) {
      expressionList.add(arg.toString());
    }
  }
}
*/
