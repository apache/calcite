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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.TranslatableTable;
import net.hydromatic.optiq.impl.AbstractTable;
import net.hydromatic.optiq.impl.TableInSchemaImpl;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.JavaRules;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.util.Pair;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class TableInRootSchemaTest {
  /** Test case for issue 85, "Adding a table to the root schema causes breakage
   * in OptiqPrepareImpl". */
  @Test public void testAddingTableInRootSchema() throws Exception {
    Class.forName("net.hydromatic.optiq.jdbc.Driver");
    Connection connection = DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection optiqConnection = connection.unwrap(OptiqConnection.class);

    SimpleTable.create(optiqConnection.getRootSchema(), "SAMPLE");

    Statement statement = optiqConnection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("select A, SUM(B) from SAMPLE group by A");

    assertThat(
        "A=foo; EXPR$1=8\n"
        + "A=bar; EXPR$1=4\n",
        equalTo(OptiqAssert.toString(resultSet)));
    resultSet.close();
    statement.close();
    connection.close();
  }

  public static class SimpleTable extends AbstractTable<Object[]>
      implements TranslatableTable<Object[]> {
    private static final String[] columnNames = { "A", "B" };
    private static final Class[] columnTypes = { String.class, Integer.class };
    private Object[][] rows = new Object[3][];

    SimpleTable(Schema schema, String tableName) {
      super(schema, SimpleTable.class, deduceTypes(schema.getTypeFactory()),tableName);

      assert schema != null;
      assert tableName != null;


      rows[0] = new Object[] { "foo", 5 };
      rows[1] = new Object[] { "bar", 4 };
      rows[2] = new Object[] { "foo", 3 };
    }

    static RelDataType deduceTypes(JavaTypeFactory typeFactory) {
      int columnCount = columnNames.length;
      final List<Pair<String, RelDataType>> columnDesc =
          new ArrayList<Pair<String, RelDataType>>(columnCount);
      for (int i = 0; i < columnCount; i++) {
        final RelDataType colType = typeFactory
            .createJavaType(columnTypes[i]);
        columnDesc.add(Pair.of(columnNames[i], colType));
      }
      return typeFactory.createStructType(columnDesc);
    }

    public static SimpleTable create(MutableSchema schema, String tableName) {
      SimpleTable table = new SimpleTable(schema, tableName);
      schema.addTable(new TableInSchemaImpl(schema, tableName,
          Schema.TableType.TABLE, table));
      return table;
    }

    @Override
    public String toString() {
      return "SimpleTable {" + tableName + "}";
    }

    public QueryProvider getProvider() {
      return schema.getQueryProvider();
    }

    public Enumerator<Object[]> enumerator() {
      return enumeratorImpl(null);
    }

    private Enumerator<Object[]> enumeratorImpl(final int[] fields) {
      return new Enumerator<Object[]>() {
        private Object[] current;
        private Iterator<Object[]> iterator = Arrays.asList(rows)
            .iterator();

        public Object[] current() {
          return current;
        }

        public boolean moveNext() {
          if (iterator.hasNext()) {
            Object[] full = iterator.next();
            current = fields != null ? convertRow(full) : full;
            return true;
          } else {
            current = null;
            return false;
          }
        }

        public void reset() {
          throw new UnsupportedOperationException();
        }

        public void close() {
          // noop
        }

        private Object[] convertRow(Object[] full) {
          final Object[] objects = new Object[fields.length];
          for (int i = 0; i < fields.length; i++) {
            objects[i] = full[fields[i]];
          }
          return objects;
        }
      };
    }

    // keep
    public RelNode toRel(RelOptTable.ToRelContext context,
        RelOptTable relOptTable) {
      return new JavaRules.EnumerableTableAccessRel(context.getCluster(),
          context.getCluster().traitSetOf(
              EnumerableConvention.INSTANCE), relOptTable,
          getExpression(), getElementType().getClass());
    }
  }
}

// End TableInRootSchemaTest.java
