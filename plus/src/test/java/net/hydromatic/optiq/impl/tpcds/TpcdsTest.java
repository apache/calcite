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
package net.hydromatic.optiq.impl.tpcds;

import net.hydromatic.optiq.test.OptiqAssert;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Random;

import net.hydromatic.tpcds.query.Query;


/** Unit test for {@link net.hydromatic.optiq.impl.tpcds.TpcdsSchema}.
 *
 * <p>Only runs if {@code -Doptiq.test.slow=true} is specified on the
 * command-line.
 * (See {@link net.hydromatic.optiq.test.OptiqAssert#ENABLE_SLOW}.)</p> */
public class TpcdsTest {
  private static String schema(String name, String scaleFactor) {
    return "     {\n"
        + "       type: 'custom',\n"
        + "       name: '" + name + "',\n"
        + "       factory: 'net.hydromatic.optiq.impl.tpcds.TpcdsSchemaFactory',\n"
        + "       operand: {\n"
        + "         columnPrefix: true,\n"
        + "         scale: " + scaleFactor + "\n"
        + "       }\n"
        + "     }";
  }

  public static final String TPCDS_MODEL =
      "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'TPCDS',\n"
      + "   schemas: [\n"
      + schema("TPCDS", "1.0") + ",\n"
      + schema("TPCDS_01", "0.01") + ",\n"
      + schema("TPCDS_5", "5.0") + "\n"
      + "   ]\n"
      + "}";

  private OptiqAssert.AssertThat with() {
    return OptiqAssert.that()
        .withModel(TPCDS_MODEL)
        .enable(OptiqAssert.ENABLE_SLOW);
  }

  @Test public void testCallCenter() {
    with()
        .query("select * from tpcds.call_center")
        .returnsUnordered();
  }

  @Ignore("add tests like this that count each table")
  @Test public void testLineItem() {
    with()
        .query("select * from tpcds.lineitem")
        .returnsCount(6001215);
  }

  /** Tests the customer table with scale factor 5. */
  @Ignore("add tests like this that count each table")
  @Test public void testCustomer5() {
    with()
        .query("select * from tpcds_5.customer")
        .returnsCount(750000);
  }

  @Ignore("assert fail in registerQuery")
  @Test public void testQuery01() {
    checkQuery(1);
  }

  @Ignore("takes too long to optimize")
  @Test public void testQuery72() {
    checkQuery(72);
  }

  private void checkQuery(int i) {
    final Query query = Query.of(i);
    String sql = query.sql(-1, new Random(0));
    switch (i) {
    case 72:
      // Work around OPTIQ-304: Support '<DATE> + <INTEGER>'.
      sql = sql.replace("+ 5", "+ interval '5' day");
    }
    with()
        .query(sql.replaceAll("tpcds\\.", "tpcds_01."))
        .runs();
  }
}

// End TpcdsTest.java
