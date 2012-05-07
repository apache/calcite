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
package org.eigenbase.rel.jdbc;

import java.util.List;

import javax.sql.*;

import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.util.SqlString;
import org.eigenbase.util.*;


/**
 * A <code>JdbcQuery</code> is a relational expression whose source is a SQL
 * statement executed against a JDBC data source. It has {@link
 * CallingConvention#RESULT_SET result set calling convention}.
 *
 * @author jhyde
 * @version $Id$
 * @since 2 August, 2002
 */
public class JdbcQuery
    extends AbstractRelNode
    implements ResultSetRel
{
    //~ Instance fields --------------------------------------------------------

    private final DataSource dataSource;

    /**
     * The expression which yields the connection object.
     */
    protected RelOptConnection connection;
    SqlDialect dialect;
    SqlSelect sql;

    /**
     * For debug. Set on register.
     */
    protected SqlString queryString;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a <code>JdbcQuery</code>.
     *
     * @param cluster {@link RelOptCluster}  this relational expression belongs
     * to
     * @param connection a {@link RelOptConnection}; must also implement {@link
     * DataSource}, because that's how we will acquire the JDBC connection
     * @param sql SQL parse tree, may be null, otherwise must be a SELECT
     * statement
     * @param dataSource Provides a JDBC connection to run this query against.
     *
     * <p>In saffron, if the query is implementing a JDBC table, then the
     * connection's schema will implement <code>
     * net.sf.saffron.ext.JdbcSchema</code>, and data source will typically be
     * the same as calling the <code>getDataSource()</code> method on that
     * schema. But non-JDBC schemas are also acceptable.
     *
     * @pre connection != null
     * @pre sql == null || sql.isA(SqlNode.Kind.Select)
     * @pre dataSource != null
     */
    public JdbcQuery(
        RelOptCluster cluster,
        RelDataType rowType,
        RelOptConnection connection,
        SqlDialect dialect,
        SqlSelect sql,
        DataSource dataSource)
    {
        super(
            cluster,
            cluster.traitSetOf(CallingConvention.RESULT_SET));
        Util.pre(connection != null, "connection != null");
        Util.pre(dataSource != null, "dataSource != null");
        this.rowType = rowType;
        this.connection = connection;
        this.dialect = dialect;
        if (sql == null) {
            sql =
                SqlStdOperatorTable.selectOperator.createCall(
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    SqlParserPos.ZERO);
        } else {
            Util.pre(
                sql.getKind() == SqlKind.SELECT,
                "sql == null || sql.isA(SqlNode.Kind.Select)");
        }
        this.sql = sql;
        this.dataSource = dataSource;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the connection
     *
     * @return connection
     */
    public RelOptConnection getConnection()
    {
        return connection;
    }

    /**
     * Returns the JDBC data source
     *
     * @return data source
     */
    public DataSource getDataSource()
    {
        return dataSource;
    }

    /**
     * @return the SQL dialect understood by the data source
     */
    public SqlDialect getDialect()
    {
        return dialect;
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "foreignSql" },
            new Object[] { getForeignSql() });
    }

    /**
     * Returns the SQL that this query will execute against the foreign
     * database, in the SQL dialect of that database.
     *
     * @return foreign SQL
     *
     * @see #getSql()
     */
    public SqlString getForeignSql()
    {
        if (queryString == null) {
            queryString = sql.toSqlString(dialect);
        }
        return queryString;
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        assert traitSet.comprises(CallingConvention.RESULT_SET);
        return new JdbcQuery(
            getCluster(),
            rowType,
            connection,
            dialect,
            sql,
            dataSource);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // Very difficult to estimate the cost of a remote query: (a) we don't
        // know what plans are available to the remote RDBMS, (b) we don't
        // know relative speed of the other CPU, or the bandwidth. This
        // estimate selfishly deals with the cost to THIS system, but it still
        // neglects the effects of latency.
        double rows = RelMetadataQuery.getRowCount(this) / 2;

        // Very difficult to estimate the cost of a remote query: (a) we don't
        // know what plans are available to the remote RDBMS, (b) we don't
        // know relative speed of the other CPU, or the bandwidth. This
        // estimate selfishly deals with the cost to THIS system, but it still
        // neglects the effects of latency.
        double cpu = 0;

        // Very difficult to estimate the cost of a remote query: (a) we don't
        // know what plans are available to the remote RDBMS, (b) we don't
        // know relative speed of the other CPU, or the bandwidth. This
        // estimate selfishly deals with the cost to THIS system, but it still
        // neglects the effects of latency.
        double io = 0 /*rows*/;
        return planner.makeCost(rows, cpu, io);
    }

    public RelNode onRegister(RelOptPlanner planner)
    {
        JdbcQuery r = (JdbcQuery) super.onRegister(planner);
        Util.discard(r.getForeignSql()); // compute query string now
        return r;
    }

    /**
     * Registers any planner rules needed to implement queries using JdbcQuery
     * objects.
     *
     * @param planner Planner
     */
    public static void register(RelOptPlanner planner)
    {
        // nothing for now
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        // Generate
        //   ((javax.sql.DataSource) connection).getConnection().
        //       createStatement().executeQuery(<<query string>>);
        //
        // The above assumes that the datasource expression is the default,
        // namely
        //
        //   (javax.sql.DataSource) connection
        //
        // Issue#1. We should really wrap this in
        //
        // Statement statement = null;
        // try {
        //   ...
        //   statement = connection.getConnection.createStatement();
        //   ...
        // } catch (java.sql.SQLException e) {
        //    throw new saffron.runtime.SaffronError(e);
        // } finally {
        //    if (stmt != null) {
        //       try {
        //          stmt.close();
        //       } catch {}
        //    }
        // }
        //
        // This is all a horrible hack. Need a way to 'freeze' a DataSource
        // into a Java expression which can be 'thawed' into a DataSource
        // at run-time. We should use the OJConnectionRegistry somehow.
        // This is all old Saffron stuff; Farrago uses its own
        // mechanism which works just fine.
        assert dataSource instanceof JdbcDataSource; // hack

        // DriverManager.getConnection("jdbc...", "scott", "tiger");
        final String url = ((JdbcDataSource) dataSource).getUrl();
        final MethodCall connectionExpr =
            new MethodCall(
                OJUtil.typeNameForClass(java.sql.DriverManager.class),
                "getConnection",
                new ExpressionList(
                    Literal.makeLiteral(url),
                    Literal.makeLiteral("SA"),
                    Literal.makeLiteral("")));
        return new MethodCall(
            new MethodCall(connectionExpr, "createStatement", null),
            "executeQuery",
            new ExpressionList(Literal.makeLiteral(queryString.getSql())));
    }

    /**
     * Returns the parse tree of the SQL statement that populates this query.
     *
     * @return SQL query
     */
    public SqlSelect getSql()
    {
        return sql;
    }
}

// End JdbcQuery.java
