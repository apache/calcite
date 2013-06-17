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
package net.hydromatic.optiq.test;

import mondrian.test.data.FoodMartData;

import org.eigenbase.util.IntegerIntervalSet;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.*;
import java.util.*;

/**
 * Test case that runs the FoodMart reference queries.
 */
@RunWith(Parameterized.class)
public class FoodmartTest {
  private static final Map<Integer, FoodmartQuery> queries =
      new LinkedHashMap<Integer, FoodmartQuery>();

  private static final int[] DISABLED_IDS = {
      58, 83, 202, 204, 205, 206, 207, 209, 211, 231, 247, 275, 309, 383, 384,
      385, 448, 449, 471, 494, 495, 496, 497, 499, 500, 501, 502, 503, 505, 506,
      507, 514, 516, 518, 520, 534, 551, 563, 566, 571, 628, 629, 630, 644, 649,
      650, 651, 653, 654, 655, 656, 657, 658, 659, 669, 722, 731, 732, 737, 748,
      750, 756, 774, 777, 778, 779, 781, 782, 783, 811, 818, 819, 820, 2057,
      2059, 2060, 2073, 2088, 2098, 2099, 2136, 2151, 2158, 2162, 2163, 2164,
      2165, 2166, 2167, 2168, 2169, 2170, 2171, 2172, 2173, 2174, 2175, 2176,
      2177, 2178, 2179, 2180, 2181, 2187, 2190, 2191, 2235, 2245, 2264, 2265,
      2266, 2267, 2268, 2270, 2271, 2279, 2327, 2328, 2341, 2356, 2365, 2374,
      2415, 2416, 2424, 2432, 2455, 2456, 2457, 2518, 2521, 2528, 2542, 2570,
      2578, 2579, 2580, 2581, 2594, 2598, 2749, 2774, 2778, 2780, 2781, 2786,
      2787, 2790, 2791, 2876, 2883, 5226, 5227, 5228, 5229, 5230, 5238, 5239,
      5249, 5279, 5281, 5282, 5283, 5284, 5286, 5288, 5291, 5415, 5444, 5445,
      5446, 5447, 5448, 5452, 5459, 5460, 5461, 5517, 5519, 5558, 5560, 5561,
      5562, 5572, 5573, 5576, 5577, 5607, 5644, 5648, 5657, 5664, 5665, 5667,
      5671, 5682, 5700, 5743, 5748, 5749, 5750, 5751, 5775, 5776, 5777, 5785,
      5793, 5796, 5797, 5810, 5811, 5814, 5816, 5852, 5874, 5875, 5910, 5953,
      5960, 5971, 5975, 5983, 6016, 6028, 6030, 6031, 6033, 6034, 6081, 6082,
      6083, 6084, 6085, 6086, 6087, 6088, 6089, 6090, 6097, 6098, 6099, 6100,
      6101, 6102, 6103, 6104, 6105, 6106, 6107, 6108, 6109, 6110, 6111, 6112,
      6113, 6114, 6115, 6141, 6150, 6156, 6160, 6164, 6168, 6169, 6172, 6177,
      6180, 6181, 6185, 6187, 6188, 6190, 6191, 6193, 6194, 6196, 6197, 6199,
      6200, 6202, 6203, 6205, 6206, 6208, 6209, 6211, 6212, 6214, 6215, 6217,
      6218, 6220, 6221, 6223, 6224, 6226, 6227, 6229, 6230, 6232, 6233, 6235,
      6236, 6238, 6239, 6241, 6242, 6244, 6245, 6247, 6248, 6250, 6251, 6253,
      6254, 6256, 6257, 6259, 6260, 6262, 6263, 6265, 6266, 6268, 6269,

      // failed
      5677, 5681,

      // 2nd run
      6271, 6272, 6274, 6275, 6277, 6278, 6280, 6281, 6283, 6284, 6286, 6287,
      6289, 6290, 6292, 6293, 6295, 6296, 6298, 6299, 6301, 6302, 6304, 6305,
      6307, 6308, 6310, 6311, 6313, 6314, 6316, 6317, 6319, 6327, 6328, 6337,
      6338, 6339, 6341, 6345, 6346, 6348, 6349, 6354, 6355, 6359, 6366, 6368,
      6369, 6375, 6376, 6377, 6389, 6396, 6400, 6422, 6424, 6445, 6447, 6449,
      6450, 6454, 6456, 6470, 6479, 6480, 6491, 6509, 6518, 6522, 6561, 6562,
      6564, 6566, 6578, 6581, 6582, 6583, 6587, 6591, 6594, 6603, 6610, 6613,
      6615, 6618, 6619, 6622, 6627, 6632, 6635, 6643, 6650, 6651, 6652, 6653,
      6656, 6659, 6668, 6670, 6720, 6726, 6735, 6737, 6739,
  };

  // Interesting tests. (We need to fix and remove from the disabled list.)
  // 5677, 5681 only: assert into Context.toSql
  // 2452, 2453, 2454, 2457 only: RTRIM
  // 2436-2453,2455: agg_
  // 2518, 5960 only: "every derived table must have its own alias"
  // 2542: timeout. Running big, simple SQL cartesian product.
  //

  // 202 and others: strip away "CAST(the_year AS UNSIGNED) = 1997"

  private final FoodmartQuery query;

  @Parameterized.Parameters(name = "{index}: foodmart({0})={1}")
  public static List<Object[]> getSqls() throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

    final InputStream inputStream = new FoodMartData().getQueries();
    FoodmartRoot root = mapper.readValue(inputStream, FoodmartRoot.class);
    final List<Object[]> list = new ArrayList<Object[]>();
      final String idList = System.getProperty("optiq.ids");
    for (FoodmartQuery query : root.queries) {
      queries.put(query.id, query);
    }
    if (idList != null) {
      StringBuilder buf = new StringBuilder();
      for (int disabledId : DISABLED_IDS) {
        buf.append(",-").append(disabledId);
      }
      buf.setLength(0); // disable disable
      for (Integer id : IntegerIntervalSet.of(idList + buf)) {
        final FoodmartQuery query1 = queries.get(id);
        if (query1 != null) {
          list.add(new Object[] {id /*, query1.sql */});
        }
      }
    } else {
      for (FoodmartQuery query1 : queries.values()) {
        list.add(new Object[]{query1.id /*, query1.sql */});
      }
    }
    return list;
  }

  public FoodmartTest(int id) {
    this.query = queries.get(id);
    assert query.id == id : id + ":" + query.id;
  }

  @Test(timeout = 60000)
  public void test() {
    try {
      OptiqAssert.assertThat()
          .withModel(JdbcTest.FOODMART_MODEL)
          // .with(OptiqAssert.Config.FOODMART_CLONE)
          .withSchema("foodmart")
          .query(query.sql)
          .runs();
    } catch (Throwable e) {
      throw new RuntimeException("Test failed, id=" + query.id + ", sql="
          + query.sql, e);
    }
  }

  // JSON class
  public static class FoodmartRoot {
    public final List<FoodmartQuery> queries = new ArrayList<FoodmartQuery>();
  }

  // JSON class
  public static class FoodmartQuery {
    public int id;
    public String sql;
    public final List<FoodmartColumn> columns = new ArrayList<FoodmartColumn>();
    public final List<List> rows = new ArrayList<List>();
  }

  // JSON class
  public static class FoodmartColumn {
    public String name;
    public String type;
  }
}

// End FoodmartTest.java
