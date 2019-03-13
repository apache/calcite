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
package org.apache.calcite.materialize;

import org.apache.calcite.adapter.tpcds.TpcdsSchema;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import net.hydromatic.tpcds.query.Query;

import org.junit.Test;

import java.util.List;
import java.util.Properties;
import java.util.Random;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link LatticeSuggester}.
 */
public class TpcdsLatticeSuggesterTest {

  private String number(String s) {
    final StringBuilder b = new StringBuilder();
    int i = 0;
    for (String line : s.split("\n")) {
      b.append(++i).append(' ').append(line).append("\n");
    }
    return b.toString();
  }

  private void checkFoodMartAll(boolean evolve) throws Exception {
    final Tester t = new Tester().tpcds().withEvolve(evolve);
    for (Query query : Query.values()) {
      final String sql = query.sql(new Random(0))
          .replaceAll("as returns", "as \"returns\"")
          .replaceAll("sum\\(returns\\)", "sum(\"returns\")")
          .replaceAll(", returns", ", \"returns\"")
          .replaceAll("14 days", "interval '14' day")
          .replaceAll("substr\\(([^,]*),([^,]*),([^)]*)\\)",
              "substring($1 from $2 for $3)");
      if (CalciteSystemProperty.DEBUG.value()) {
        System.out.println("Query #" + query.id + "\n"
            + number(sql));
      }
      switch (query.id) {
      case 6:
      case 9:
        continue; // NPE
      }
      if (query.id > 11) {
        break;
      }
      t.addQuery(sql);
    }

    // The graph of all tables and hops
    final String expected = "graph(vertices: ["
        + "[tpcds, CATALOG_SALES], "
        + "[tpcds, CUSTOMER], "
        + "[tpcds, CUSTOMER_ADDRESS], "
        + "[tpcds, CUSTOMER_DEMOGRAPHICS], "
        + "[tpcds, DATE_DIM], "
        + "[tpcds, ITEM], "
        + "[tpcds, PROMOTION], "
        + "[tpcds, STORE], "
        + "[tpcds, STORE_RETURNS], "
        + "[tpcds, STORE_SALES], "
        + "[tpcds, WEB_SALES]], "
        + "edges: "
        + "[Step([tpcds, CATALOG_SALES], [tpcds, CUSTOMER], CS_SHIP_CUSTOMER_SK:C_CUSTOMER_SK),"
        + " Step([tpcds, CATALOG_SALES], [tpcds, DATE_DIM], CS_SOLD_DATE_SK:D_DATE_SK),"
        + " Step([tpcds, STORE_RETURNS], [tpcds, CUSTOMER], SR_CUSTOMER_SK:C_CUSTOMER_SK),"
        + " Step([tpcds, STORE_RETURNS], [tpcds, DATE_DIM], SR_RETURNED_DATE_SK:D_DATE_SK),"
        + " Step([tpcds, STORE_RETURNS], [tpcds, STORE], SR_STORE_SK:S_STORE_SK),"
        + " Step([tpcds, STORE_RETURNS], [tpcds, STORE_RETURNS], SR_STORE_SK:SR_STORE_SK),"
        + " Step([tpcds, STORE_SALES], [tpcds, CUSTOMER], SS_CUSTOMER_SK:C_CUSTOMER_SK),"
        + " Step([tpcds, STORE_SALES], [tpcds, CUSTOMER_DEMOGRAPHICS], SS_CDEMO_SK:CD_DEMO_SK),"
        + " Step([tpcds, STORE_SALES], [tpcds, DATE_DIM], SS_SOLD_DATE_SK:D_DATE_SK),"
        + " Step([tpcds, STORE_SALES], [tpcds, ITEM], SS_ITEM_SK:I_ITEM_SK),"
        + " Step([tpcds, STORE_SALES], [tpcds, PROMOTION], SS_PROMO_SK:P_PROMO_SK),"
        + " Step([tpcds, WEB_SALES], [tpcds, CUSTOMER], WS_BILL_CUSTOMER_SK:C_CUSTOMER_SK),"
        + " Step([tpcds, WEB_SALES], [tpcds, DATE_DIM], WS_SOLD_DATE_SK:D_DATE_SK)])";
    assertThat(t.suggester.space.g.toString(), is(expected));
    if (evolve) {
      assertThat(t.suggester.space.nodeMap.size(), is(5));
      assertThat(t.suggester.latticeMap.size(), is(3));
      assertThat(t.suggester.space.pathMap.size(), is(10));
    } else {
      assertThat(t.suggester.space.nodeMap.size(), is(5));
      assertThat(t.suggester.latticeMap.size(), is(4));
      assertThat(t.suggester.space.pathMap.size(), is(10));
    }
  }

  @Test public void testTpcdsAll() throws Exception {
    checkFoodMartAll(false);
  }

  @Test public void testTpcdsAllEvolve() throws Exception {
    checkFoodMartAll(true);
  }

  /** Test helper. */
  private static class Tester {
    final LatticeSuggester suggester;
    private final FrameworkConfig config;

    Tester() {
      this(config(CalciteAssert.SchemaSpec.BLANK).build());
    }

    private Tester(FrameworkConfig config) {
      this.config = config;
      suggester = new LatticeSuggester(config);
    }

    Tester tpcds() {
      final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
      final double scaleFactor = 0.01d;
      final SchemaPlus schema =
          rootSchema.add("tpcds", new TpcdsSchema(scaleFactor));
      final FrameworkConfig config = Frameworks.newConfigBuilder()
          .parserConfig(SqlParser.Config.DEFAULT)
          .context(
              Contexts.of(
                  new CalciteConnectionConfigImpl(new Properties())
                      .set(CalciteConnectionProperty.CONFORMANCE,
                          SqlConformanceEnum.LENIENT.name())))
          .defaultSchema(schema)
          .build();
      return withConfig(config);
    }

    Tester withConfig(FrameworkConfig config) {
      return new Tester(config);
    }

    List<Lattice> addQuery(String q) throws SqlParseException,
        ValidationException, RelConversionException {
      final Planner planner = new PlannerImpl(config);
      final SqlNode node = planner.parse(q);
      final SqlNode node2 = planner.validate(node);
      final RelRoot root = planner.rel(node2);
      return suggester.addQuery(root.project());
    }

    /** Parses a query returns its graph. */
    LatticeRootNode node(String q) throws SqlParseException,
        ValidationException, RelConversionException {
      final List<Lattice> list = addQuery(q);
      assertThat(list.size(), is(1));
      return list.get(0).rootNode;
    }

    static Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec spec) {
      final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
      final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, spec);
      return Frameworks.newConfigBuilder()
          .parserConfig(SqlParser.Config.DEFAULT)
          .defaultSchema(schema);
    }

    Tester withEvolve(boolean evolve) {
      if (evolve == config.isEvolveLattice()) {
        return this;
      }
      final Frameworks.ConfigBuilder configBuilder =
          Frameworks.newConfigBuilder(config);
      return new Tester(configBuilder.evolveLattice(true).build());
    }
  }
}

// End TpcdsLatticeSuggesterTest.java
