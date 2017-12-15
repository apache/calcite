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
package org.apache.calcite.runtime.mr;

import org.apache.calcite.runtime.SqlFunctions;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * match recognize util functions
 */
public class MRUtilFuns {
  public static final String STAR = "*";

  /**
   * private constructor
   */
  private MRUtilFuns() {
  }

  public static String objectsToS(Object[] array) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < array.length; i++) {
      if (array[i] instanceof Number) {
        builder.append(array[i]);
      } else {
        builder.append(array[i].toString());
      }
      if (i != array.length - 1) {
        builder.append(",");
      }
    }
    return builder.toString();
  }

  public static Object getAttrAt(Map<Integer, Tuple> tuples, int tid, int idx) {
    if (tuples.containsKey(tid)) {
      return tuples.get(tid).getData(idx);
    }
    return null;
  }

  public static Object getAggrValueAt(Matching matching,
                                      String aggrFun,
                                      String type,
                                      int idx,
                                      boolean isFinal) {
    List list = matching.getAggrResult(aggrFun);
    int idxInList = isFinal ? list.size() - 1 : matching.getTuples().indexOf(idx);
    Object result = idxInList < 0 ? null : list.get(idxInList);
    AggregateEnum aggType = AggregateEnum.valueOf(type);
    if (result == null) {
      switch (aggType) {
      case COUNT:
      case COUNT_STAR:
        result = Long.valueOf(0);
        break;
      case SUM:
        result = BigDecimal.ZERO;
        break;
      }
    }
    return result;
  }

  public static void computeAggrValue(Matching matching, String fun, String kind, Object newValue) {
    computeAggrValue(matching, fun, kind, newValue, false);
  }

  public static Object computeAggrValue(String kind, Object oldValue, Object newValue) {
    Object result = null;
    AggregateEnum type = AggregateEnum.valueOf(kind);
    switch (type) {
    case COUNT_STAR:
      oldValue = oldValue == null ? new Long(0) : SqlFunctions.toLong((Number) oldValue);
      result = SqlFunctions.plus((Long) oldValue, 1);
      break;
    case COUNT:
      oldValue = oldValue == null ? new Long(0) : SqlFunctions.toLong((Number) oldValue);
      result = newValue == null ? oldValue : SqlFunctions.plus((Long) oldValue, 1);
      break;
    case SUM:
      oldValue = oldValue == null ? new BigDecimal(0) : oldValue;
      result = SqlFunctions.plus((BigDecimal) oldValue, (BigDecimal) newValue);
      break;
    case MAX:
      result = oldValue == null ? newValue
          : (SqlFunctions.lt((BigDecimal) oldValue, (BigDecimal) newValue) ? newValue : oldValue);
      break;
    case MIN:
      result = oldValue == null ? newValue
          : (SqlFunctions.gt((BigDecimal) oldValue, (BigDecimal) newValue) ? newValue : oldValue);
      break;
    default:
      new AssertionError("Invalid Function Type " + kind);
    }
    return result;
  }

  public static void computeAggrValue(Matching matching,
                                      String fun,
                                      String kind,
                                      Object newValue,
                                      boolean isAllRows) {
    List list = matching.getAggrResult(fun);
    Object oldValue = list.size() == 0 ? null : list.get(list.size() - 1);
    Object result = computeAggrValue(kind, oldValue, newValue);
    if (isAllRows || list.size() == 0) {
      list.add(result);
    } else {
      list.set(0, result);
    }
  }

  public static void copyAndAppend(Matching matching, String fun, String type) {
    List list = matching.getAggrResult(fun);
    Object lastValue = list.size() == 0 ? null : list.get(list.size() - 1);
    AggregateEnum aggType = AggregateEnum.valueOf(type);
    if (lastValue == null) {
      switch (aggType) {
      case COUNT:
      case COUNT_STAR:
        lastValue = Long.valueOf(0);
        break;
      case SUM:
        lastValue = BigDecimal.ZERO;
        break;
      }
    }
    list.add(lastValue);
  }

  public static int first(Matching matching, String targetAlpha, String alpha, int offSet) {
    List<Integer> list = alpha.equals(STAR) ? matching.getTuples() : matching.getClassifier(alpha);
    if (list == null && targetAlpha != null && alpha.equals(targetAlpha)) {
      return matching.getNextTID();
    }
    int idx = list != null && list.size() > 0 && offSet < list.size() ? list.get(offSet) : -1;
    return idx;
  }

  public static int first(Matching matching,
                          String targetAlpha,
                          String alpha,
                          int offSet,
                          boolean isFinal,
                          int idx) {
    return first(matching, targetAlpha, alpha, offSet);
  }

  public static int last(Matching matching,
                         String targetAlpha,
                         String alpha,
                         int offset,
                         boolean isFinal,
                         int idx) {
    List<Integer> list = alpha.equals(STAR) ? matching.getTuples() : matching.getClassifier(alpha);
    int result = -1;

    if (isFinal) {
      result = list != null && list.size() > 0 ? list.get(list.size() - offset - 1) : -1;
      return result;
    }

    // eg : DEFINE A as A.price >= FIRST(A.price), here FIRST(A.price) == A.price
    if (list == null && targetAlpha != null && alpha.equals(targetAlpha)) {
      return result;
    }

    if (list != null && list.size() > 0) {
      int firstID = list.get(0);
      if (idx < firstID) {
        return -1;
      }
      int rowIdx = list.indexOf(idx);
      if (rowIdx >= 0) {
        result = rowIdx < offset ? -1 : list.get(rowIdx - offset);
      } else {
        result = list.get(list.size() - offset - 1);
      }
      return result;
    }
    return -1;
  }

  public static int prev(int idx, int offSet) {
    return idx - offSet;
  }

  public static int next(int idx, int offSet) {
    return idx + offSet;
  }

  public static int prev(int idx) {
    return prev(idx, 1);
  }

  public static int next(int idx) {
    return next(idx, 1);
  }

  public static Map<Integer, Tuple> cleanUp(final Map<Integer, Tuple> tuples,
                                            Map<String, Map<Integer, Integer>> tuplesPerPart,
                                            int smallestTID) {
    //remove tuples that will never be visited again
    if (smallestTID == 0) {
      return tuples;
    }
    int minTID = tuples.size() > 0 ? Collections.min(tuples.keySet()) : 0;
    if (minTID >= smallestTID) {
      return tuples;
    }
    Map<Integer, Tuple> tmpTuples = new HashMap<>();
    for (Map.Entry<Integer, Tuple> et : tuples.entrySet()) {
      if (et.getKey() >= smallestTID) {
        tmpTuples.put(et.getKey(), et.getValue());
      }
    }

    Set<String> partKeys = tuplesPerPart.keySet();
    for (String key : partKeys) {
      Map<Integer, Integer> map = tuplesPerPart.get(key);
      if (Collections.min(map.values()) < smallestTID) {
        Map<Integer, Integer> tmpMap = new HashMap<>();
        for (Map.Entry<Integer, Integer> et : map.entrySet()) {
          if (et.getKey() >= smallestTID) {
            tmpMap.put(et.getKey(), et.getValue());
          }
        }
        tuplesPerPart.put(key, tmpMap);
      }
    }
    return tmpTuples;
  }

}

// End MRUtilFuns.java
