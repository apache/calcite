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
package org.apache.calcite.adapter.druid;


import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;


/**
 * Utility class for extraction function mapping between SQL and Druid.
 */
public final class ExtractionFunctionUtil {

  private ExtractionFunctionUtil() {
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * This method will be used to build a Druid extraction function out of a SQL EXTRACT rexNode.
   *
   * @param rexNode node that might contain an extraction function on time
   * @return the correspondent Druid extraction function or null if it is not recognisable
   */
  public static ExtractionFunction buildExtraction(RexNode rexNode) {
    if (rexNode instanceof RexCall) {
      RexCall call = (RexCall) rexNode;
      if (call.getKind().equals(SqlKind.EXTRACT)) {
        final RexLiteral flag = (RexLiteral) call.operands.get(0);
        final TimeUnitRange timeUnit = (TimeUnitRange) flag.getValue();
        if (timeUnit == null) {
          return null;
        }
        switch (timeUnit) {
        case YEAR:
          return TimeExtractionFunction.createFromGranularity(Granularity.YEAR);
        case MONTH:
          return TimeExtractionFunction.createFromGranularity(Granularity.MONTH);
        case DAY:
          return TimeExtractionFunction.createFromGranularity(Granularity.DAY);
        default:
          return null;
        }
      }
    }
    return null;
  }
}

// End ExtractionFunctionUtil.java
