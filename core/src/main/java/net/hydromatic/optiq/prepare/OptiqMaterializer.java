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
package net.hydromatic.optiq.prepare;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.StarTable;
import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.jdbc.OptiqSchema;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.EnumerableRel;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql2rel.SqlToRelConverter;

import java.util.*;

/**
* Context for populating a {@link Materialization}.
*/
class OptiqMaterializer extends OptiqPrepareImpl.OptiqPreparingStmt {
  public OptiqMaterializer(OptiqPrepare.Context context,
      CatalogReader catalogReader, OptiqSchema schema,
      RelOptPlanner planner) {
    super(context, catalogReader, catalogReader.getTypeFactory(), schema,
        EnumerableRel.Prefer.ANY, planner, EnumerableConvention.INSTANCE);
  }

  /** Populates a materialization record, converting a table path
   * (essentially a list of strings, like ["hr", "sales"]) into a table object
   * that can be used in the planning process. */
  void populate(Materialization materialization) {
    SqlParser parser = SqlParser.create(materialization.sql);
    SqlNode node;
    try {
      node = parser.parseStmt();
    } catch (SqlParseException e) {
      throw new RuntimeException("parse failed", e);
    }

    SqlToRelConverter sqlToRelConverter2 =
        getSqlToRelConverter(getSqlValidator(), catalogReader);

    materialization.queryRel =
        sqlToRelConverter2.convertQuery(node, true, true);

    // Identify and substitute a StarTable in queryRel.
    //
    // It is possible that no StarTables match. That is OK, but the
    // materialization patterns that are recognized will not be as rich.
    //
    // It is possible that more than one StarTable matches. TBD: should we
    // take the best (whatever that means), or all of them?
    useStar(schema, materialization);

    RelOptTable table =
        this.catalogReader.getTable(materialization.materializedTable.path());
    materialization.tableRel = sqlToRelConverter2.toRel(table);
  }

  /** Converts a relational expression to use a
   * {@link net.hydromatic.optiq.impl.StarTable} defined in {@code schema}.
   * Uses the first star table that fits. */
  private void useStar(OptiqSchema schema, Materialization materialization) {
    List<OptiqSchema.TableEntry> starTables = getStarTables(schema);
    if (starTables.isEmpty()) {
      // Don't waste effort converting to leaf-join form.
      return;
    }
    final RelNode rel2 =
        RelOptMaterialization.toLeafJoinForm(materialization.queryRel);
    for (OptiqSchema.TableEntry starTable : starTables) {
      final Table table = starTable.getTable();
      assert table instanceof StarTable;
      RelOptTableImpl starRelOptTable =
          RelOptTableImpl.create(catalogReader, table.getRowType(typeFactory),
              starTable);
      final RelNode rel3 =
          RelOptMaterialization.tryUseStar(rel2, starRelOptTable);
      if (rel3 != null) {
        // Success -- we found a star table that matches.
        materialization.materialize(rel3, starRelOptTable);
        System.out.println("Materialization "
            + materialization.materializedTable + " matched star table "
            + starTable + "; query after re-write: "
            + RelOptUtil.toString(materialization.queryRel));
        return;
      }
    }
  }

  /** Returns the star tables defined in a schema.
   * @param schema Schema */
  private List<OptiqSchema.TableEntry> getStarTables(OptiqSchema schema) {
    final List<OptiqSchema.TableEntry> list =
        new ArrayList<OptiqSchema.TableEntry>();
    // TODO: Assumes that star tables are all defined in a schema called
    // "mat". Instead, we should look for star tables that use a given set of
    // tables, regardless of schema.
    final OptiqSchema matSchema = schema.root().getSubSchema("mat", true);
    if (matSchema != null) {
      for (OptiqSchema.TableEntry tis : matSchema.getTableEntries()) {
        if (tis.getTable().getJdbcTableType() == Schema.TableType.STAR) {
          list.add(tis);
        }
      }
    }
    return list;
  }

  /** Implementation of {@link RelShuttle} that returns each relational
   * expression unchanged. It does not visit children. */
  static class RelNullShuttle implements RelShuttle {
    public RelNode visit(TableAccessRelBase scan) {
      return scan;
    }
    public RelNode visit(TableFunctionRelBase scan) {
      return scan;
    }
    public RelNode visit(ValuesRel values) {
      return values;
    }
    public RelNode visit(FilterRel filter) {
      return filter;
    }
    public RelNode visit(ProjectRel project) {
      return project;
    }
    public RelNode visit(JoinRel join) {
      return join;
    }
    public RelNode visit(UnionRel union) {
      return union;
    }
    public RelNode visit(IntersectRel intersect) {
      return intersect;
    }
    public RelNode visit(MinusRel minus) {
      return minus;
    }
    public RelNode visit(AggregateRel aggregate) {
      return aggregate;
    }
    public RelNode visit(SortRel sort) {
      return sort;
    }
    public RelNode visit(RelNode other) {
      return other;
    }
  }

}

// End OptiqMaterializer.java
