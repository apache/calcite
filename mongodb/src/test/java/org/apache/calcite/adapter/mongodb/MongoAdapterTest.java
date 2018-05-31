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
package org.apache.calcite.adapter.mongodb;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.test.CalciteAssert;

import com.github.fakemongo.junit.FongoRule;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.BsonDocument;
import org.bson.Document;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Tests current adapter using in-memory (fake) implementation of Mongo API:
 * <a href="https://github.com/fakemongo/fongo">Fongo</a>.
 *
 */
public class MongoAdapterTest {

  @Rule
  public final FongoRule rule = new FongoRule();

  private MongoDatabase mongoDb;
  private MongoCollection<Document> zips;

  @Before
  public void setUp() throws Exception {
    mongoDb = rule.getDatabase(getClass().getSimpleName());
    zips = mongoDb.getCollection("zips");
  }

  /**
   * Handcrafted connection where we manually added {@link MongoSchema}
   */
  private CalciteAssert.ConnectionFactory newConnectionFactory() {
    return new CalciteAssert.ConnectionFactory() {
      @Override public Connection createConnection() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        final SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();
        root.add("mongo", new MongoSchema(mongoDb));
        return connection;
      }
    };
  }

  @Test
  public void single() {
    zips.insertOne(new Document());
    CalciteAssert.that()
            .with(newConnectionFactory())
            .query("select * from \"mongo\".\"zips\"")
            .runs()
            .returnsCount(1);
  }

  @Test
  public void empty() {
    // for some reason fongo doesn't list collection if it was unused
    zips.insertOne(new Document());
    zips.deleteMany(new BsonDocument());

    CalciteAssert.that()
            .with(newConnectionFactory())
            .query("select * from \"mongo\".\"zips\"")
            .runs()
            .returnsCount(0);
  }

  @Test
  public void filter() {
    zips.insertOne(new Document("CITY", "New York").append("STATE", "NY"));
    zips.insertOne(new Document("CITY", "Washington").append("STATE", "DC"));

    CalciteAssert.that()
            .with(newConnectionFactory())
            .query("select cast(_MAP['CITY'] as varchar(20)) as \"city\" from \"mongo\".\"zips\" "
                    + " where _MAP['STATE'] = 'NY'")
            .returns("city=New York\n");

    CalciteAssert.that()
            .with(newConnectionFactory())
            .query("select cast(_MAP['CITY'] as varchar(20)) as \"city\" from \"mongo\".\"zips\" "
                   + " where _MAP['STATE'] = 'DC'")
            .returns("city=Washington\n");

    CalciteAssert.that()
            .with(newConnectionFactory())
            .query("select cast(_MAP['CITY'] as varchar(20)) as \"city\" from \"mongo\".\"zips\" "
                    + " where _MAP['STATE'] in ('DC', 'NY')")
            .returns("city=New York\ncity=Washington\n");
  }

  @Test
  public void limit() {
    zips.insertOne(new Document("CITY", "New York").append("STATE", "NY"));
    zips.insertOne(new Document("CITY", "Washington").append("STATE", "DC"));

    CalciteAssert.that()
            .with(newConnectionFactory())
            .query("select * from \"mongo\".\"zips\" limit 1")
            .returnsCount(1);

    CalciteAssert.that()
            .with(newConnectionFactory())
            .query("select * from \"mongo\".\"zips\" limit 2")
            .returnsCount(2);

  }

  /**
   * Following queries are not supported in Mongo adapter :
   * <pre>
   * {@code A and (B or C)}
   * {@code (A or B) and C}
   * </pre>
   *
   * @see <a href="https://issues.apache.org/jira/browse/CALCITE-2331">[CALCITE-2331]</a>
   */
  @Ignore("broken; [CALCITE-2331] is logged to fix it")
  @Test
  public void validateCALCITE2331() {
    zips.insertOne(new Document("CITY", "New York").append("STATE", "NY"));
    zips.insertOne(new Document("CITY", "Washington").append("STATE", "DC"));

    CalciteAssert.that()
            .with(newConnectionFactory())
            .query("select cast(_MAP['CITY'] as varchar(20)) as \"city\" from \"mongo\".\"zips\" "
                    + " where _MAP['STATE'] in ('DC', 'NY') and _MAP['CITY'] = 'New York'")
            .returns("city=New York\n");
  }
}

// End MongoAdapterTest.java
