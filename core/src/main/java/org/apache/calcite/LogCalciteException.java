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

import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.tools.RelBuilder;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;

/**
 * Define aspect for calcite. It will help to identify problematic code
 * easily and quickly
 */
@Aspect
public class LogCalciteException {

  private static final String BASE_PACKAGE = "org.apache.calcite";
  private static final String REL_PACKAGE = "org.apache.calcite.rel..*";
  private static final String TOOL_PACKAGE = "org.apache.calcite.tool..*";
  private static final String METADATA_PACKAGE = "org.apache.calcite.metadata..*";
  public static final String MESSAGE_PREFIX = "The issue occurred because of ";

  @AfterThrowing(pointcut =
      "execution(* " + BASE_PACKAGE + "..*(..)) && (within(" + REL_PACKAGE + ") || "
          + "within(" + TOOL_PACKAGE + "))", throwing = "ex")
  public void logException(JoinPoint joinPoint, Exception ex) {

    if (joinPoint.getTarget() != null) {
      String detail = "";
      if (joinPoint.getThis() instanceof RelBuilder
          && ((RelBuilder) joinPoint.getThis()).size() > 0) {
        detail += MESSAGE_PREFIX + "relNode "
            + ((RelBuilder) joinPoint.getThis()).peek().explain();
      } else if (joinPoint.getThis() instanceof SingleRel
          && ((SingleRel) joinPoint.getThis()).getInput() != null) {
        detail += MESSAGE_PREFIX + "relNode "
            + ((SingleRel) joinPoint.getThis()).getInput().explain();
      }
      if ("".equals(detail) && joinPoint.getArgs().length > 0) {
        detail += MESSAGE_PREFIX + "method call " + joinPoint.getSignature().toString()
            + " with args";
        for (Object arg : joinPoint.getArgs()) {
          detail += " " + arg.toString();
        }
      }
      if (!"".equals(detail)) {
        ex.addSuppressed(new RuntimeException(detail));
      }
    }
  }
}
