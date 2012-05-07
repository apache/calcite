package net.hydromatic.optiq.jdbc;

import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.Function;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaLink;
import net.hydromatic.optiq.SchemaObject;
import net.hydromatic.optiq.rules.java.JavaRules;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.rex.OJRexImplementorTableImpl;
import org.eigenbase.oj.stmt.OJPreparingStmt;
import org.eigenbase.oj.stmt.PreparedResult;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.volcano.VolcanoPlanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.sql.SqlAccessType;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.type.MultisetSqlType;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql2rel.SqlToRelConverter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author jhyde
 */
class OptiqPrepare {
    public static PrepareResult prepare(OptiqStatement statement, String sql) {
        final RelDataTypeFactory typeFactory = statement.connection.typeFactory;
        OptiqCatalogReader catalogReader =
            new OptiqCatalogReader(
                statement.connection.rootSchema,
                typeFactory);
        RelOptConnectionImpl relOptConnection =
            new RelOptConnectionImpl(catalogReader);
        final OptiqPreparingStmt preparingStmt =
            new OptiqPreparingStmt(
                relOptConnection,
                typeFactory);
        preparingStmt.setResultCallingConvention(CallingConvention.ENUMERABLE);

        SqlParser parser = new SqlParser(sql);
        SqlNode sqlNode;
        try {
            sqlNode = parser.parseQuery();
        } catch (SqlParseException e) {
            throw new RuntimeException("parse failed", e);
        }
        SqlValidator validator =
            new SqlValidatorImpl(
                SqlStdOperatorTable.instance(), catalogReader, typeFactory,
                SqlConformance.Default) { };
        PreparedResult preparedResult =
            preparingStmt.prepareSql(
                sqlNode, Object.class, validator, true);
        // TODO: parameters
        final List<OptiqParameter> parameters = Collections.emptyList();
        // TODO: column meta data
        final List<OptiqResultSetMetaData.ColumnMetaData> columns =
            Collections.emptyList();
        final OptiqResultSetMetaData resultSetMetaData =
            new OptiqResultSetMetaData(statement, null, columns);
        return new PrepareResult(
            sql, parameters, resultSetMetaData);
    }

    static class PrepareResult {
        final String sql; // for debug
        final List<OptiqParameter> parameterList;
        final OptiqResultSetMetaData resultSetMetaData;

        public PrepareResult(
            String sql,
            List<OptiqParameter> parameterList,
            OptiqResultSetMetaData resultSetMetaData)
        {
            super();
            this.sql = sql;
            this.parameterList = parameterList;
            this.resultSetMetaData = resultSetMetaData;
        }

        public OptiqEnumerator<Object> execute() {
            return null;
        }
    }

    private static class OptiqPreparingStmt extends OJPreparingStmt {
        private final RelOptPlanner planner;
        private final RexBuilder rexBuilder;

        public OptiqPreparingStmt(
            RelOptConnection connection, RelDataTypeFactory typeFactory)
        {
            super(connection);
            planner = new VolcanoPlanner();
            planner.addRelTraitDef(CallingConventionTraitDef.instance);
            RelOptUtil.registerAbstractRels(planner);
            planner.addRule(JavaRules.ENUMERABLE_JOIN_RULE);
            planner.addRule(JavaRules.ENUMERABLE_CALC_RULE);

            rexBuilder = new RexBuilder(typeFactory);
        }

        @Override
        protected SqlToRelConverter getSqlToRelConverter(
            SqlValidator validator, RelOptConnection connection)
        {
            return new SqlToRelConverter(
                validator,
                connection.getRelOptSchema(),
                env, planner,
                connection, rexBuilder);
        }

        @Override
        protected JavaRelImplementor getRelImplementor(RexBuilder rexBuilder) {
            return new JavaRelImplementor(
                rexBuilder, OJRexImplementorTableImpl.instance());
        }

        @Override
        protected String getClassRoot() {
            return null;
        }

        @Override
        protected String getCompilerClassName() {
            return "org.eigenbase.javac.JaninoCompiler";
        }

        @Override
        protected String getJavaRoot() {
            return null;
        }

        @Override
        protected String getTempPackageName() {
            return "foo";
        }

        @Override
        protected String getTempMethodName() {
            return null;
        }

        @Override
        protected String getTempClassName() {
            return "Foo";
        }

        @Override
        protected boolean shouldAlwaysWriteJavaFile() {
            return false;
        }

        @Override
        protected boolean shouldSetConnectionInfo() {
            return false;
        }

        @Override
        protected RelNode flattenTypes(
            RelNode rootRel,
            boolean restructure)
        {
            return rootRel;
        }

        @Override
        protected RelNode decorrelate(SqlNode query, RelNode rootRel) {
            return rootRel;
        }
    }

    private static class Table
        implements SqlValidatorTable, RelOptTable
    {
        private final RelOptSchema schema;
        private final RelDataType rowType;
        private final String[] names;
        private final List<PathElement> pathElements;

        public Table(
            RelOptSchema schema,
            RelDataType rowType,
            String[] names,
            List<PathElement> pathElements)
        {
            this.schema = schema;
            this.rowType = rowType;
            this.names = names;
            this.pathElements = pathElements;
        }

        public double getRowCount() {
            return 100;
        }

        public RelOptSchema getRelOptSchema() {
            return schema;
        }

        public RelNode toRel(
            RelOptCluster cluster,
            RelOptConnection connection)
        {
            return new JavaRules.EnumerableTableAccessRel(
                cluster, this, connection);
        }

        public List<RelCollation> getCollationList() {
            return Collections.emptyList();
        }

        public RelDataType getRowType() {
            return rowType;
        }

        public String[] getQualifiedName() {
            return names;
        }

        public List<PathElement> getPathElements() {
            return pathElements;
        }

        public SqlMonotonicity getMonotonicity(String columnName) {
            return SqlMonotonicity.NotMonotonic;
        }

        public SqlAccessType getAllowedAccess() {
            return SqlAccessType.READ_ONLY;
        }
    }

    private static class OptiqCatalogReader
        implements SqlValidatorCatalogReader, RelOptSchema
    {
        private final Schema schema;
        private final RelDataTypeFactory typeFactory;
        private final Expression rootExpression =
            Expressions.variable(Map.class, "root");

        public OptiqCatalogReader(
            Schema schema, RelDataTypeFactory typeFactory)
        {
            super();
            this.schema = schema;
            this.typeFactory = typeFactory;
        }

        public Table getTable(final String[] names) {
            final List<PathElement> pathElements = new ArrayList<PathElement>();
            Schema schema2 = schema;
            for (int i = 0; i < names.length; i++) {
                final String name = names[i];
                final SchemaObject schemaObject = schema2.get(name);
                pathElements.add(
                    PathElement.of(
                        schemaObject,
                        name,
                        Collections.<Expression>emptyList()));
                if (schemaObject instanceof Function) {
                    if (i != names.length - 1) {
                        return null;
                    }
                    RelDataType type = ((Function) schemaObject).getType();
                    if (type instanceof MultisetSqlType) {
                        return new Table(
                            this,
                            type.getComponentType(),
                            names,
                            pathElements);
                    }
                }
                if (schemaObject instanceof SchemaLink) {
                    schema2 = ((SchemaLink) schemaObject).schema;
                    continue;
                }
                return null;
            }
            return null;
        }

        public RelDataType getNamedType(SqlIdentifier typeName) {
            return null;
        }

        public List<SqlMoniker> getAllSchemaObjectNames(List<String> names) {
            return null;
        }

        public String getSchemaName() {
            return null;
        }

        public Table getTableForMember(String[] names) {
            return getTable(names);
        }

        public RelDataTypeFactory getTypeFactory() {
            return typeFactory;
        }

        public void registerRules(RelOptPlanner planner) throws Exception {
        }
    }

    private static class PathElement {
        private final SchemaObject schemaObject;
        private final String name;
        private final List<Expression> expressions;

        private PathElement(
            SchemaObject schemaObject,
            String name,
            List<Expression> expressions)
        {
            this.schemaObject = schemaObject;
            this.name = name;
            this.expressions = expressions;
        }

        public static PathElement of(
            SchemaObject schemaObject,
            String name,
            List<Expression> expressions)
        {
            return new PathElement(schemaObject, name, expressions);
        }
    }

    private static class RelOptConnectionImpl implements RelOptConnection {
        private final RelOptSchema schema;

        public RelOptConnectionImpl(RelOptSchema schema) {
            this.schema = schema;
        }

        public RelOptSchema getRelOptSchema() {
            return schema;
        }

        public Object contentsAsArray(String qualifier, String tableName) {
            return null;
        }
    }
}

// End OptiqPrepare.java
