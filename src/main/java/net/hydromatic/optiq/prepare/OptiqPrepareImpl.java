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

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.linq4j.function.Function2;
import net.hydromatic.linq4j.function.Predicate1;
import net.hydromatic.linq4j.function.Predicate2;
import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.Helper;
import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.rules.java.*;
import net.hydromatic.optiq.runtime.Executable;

import openjava.ptree.ClassDeclaration;

import org.codehaus.janino.*;

import org.eigenbase.oj.stmt.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.TableAccessRule;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.volcano.VolcanoPlanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.type.MultisetSqlType;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql2rel.SqlToRelConverter;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;

/**
 * Shit just got real.
 *
 * @author jhyde
 */
class OptiqPrepareImpl implements OptiqPrepare {
    static final Method METHOD_QUERYABLE_SELECT;
    static final Method METHOD_ENUMERABLE_SELECT;
    static final Method METHOD_ENUMERABLE_SELECT2;
    static final Method METHOD_ENUMERABLE_WHERE;
    static final Method METHOD_ENUMERABLE_WHERE2;
    static final Method METHOD_ENUMERABLE_ASQUERYABLE;

    static {
        try {
            METHOD_QUERYABLE_SELECT =
                Queryable.class.getMethod("select", FunctionExpression.class);
            METHOD_ENUMERABLE_SELECT =
                Enumerable.class.getMethod("select", Function1.class);
            METHOD_ENUMERABLE_SELECT2 =
                Enumerable.class.getMethod("select", Function2.class);
            METHOD_ENUMERABLE_WHERE =
                Enumerable.class.getMethod("where", Predicate1.class);
            METHOD_ENUMERABLE_WHERE2 =
                Enumerable.class.getMethod("where", Predicate2.class);
            METHOD_ENUMERABLE_ASQUERYABLE =
                Enumerable.class.getMethod("asQueryable");
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public PrepareResult prepare2(
        Statement statement,
        Expression expression,
        Type elementType)
    {
        return prepare(
            statement, null, expression, elementType);
    }

    public PrepareResult prepare(
        Statement statement,
        String sql,
        Expression expression,
        Type elementType)
    {
        final RelDataTypeFactory typeFactory = statement.getTypeFactory();
        OptiqCatalogReader catalogReader =
            new OptiqCatalogReader(
                statement.getRootSchema(),
                typeFactory);
        RelOptConnectionImpl relOptConnection =
            new RelOptConnectionImpl(catalogReader);
        final OptiqPreparingStmt preparingStmt =
            new OptiqPreparingStmt(
                relOptConnection,
                typeFactory,
                statement.getRootSchema(),
                statement.getRoot());
        preparingStmt.setResultCallingConvention(CallingConvention.ENUMERABLE);

        final RelDataType x;
        final PreparedResult preparedResult;
        if (sql != null) {
            assert expression == null;
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
            preparedResult = preparingStmt.prepareSql(
                sqlNode, Object.class, validator, true);
            x = validator.getValidatedNodeType(sqlNode);
        } else {
            assert expression != null;
            x = statement.getTypeFactory().createJavaType(
                Types.toClass(elementType));
            preparedResult =
                preparingStmt.prepareExpression(expression, x);
        }

        // TODO: parameters
        final List<Parameter> parameters = Collections.emptyList();
        // TODO: column meta data
        final List<ColumnMetaData> columns =
            new ArrayList<ColumnMetaData>();
        RelDataType jdbcType = makeStruct(typeFactory, x);
        for (RelDataTypeField field : jdbcType.getFields()) {
            RelDataType type = field.getType();
            SqlTypeName sqlTypeName = type.getSqlTypeName();
            columns.add(
                new ColumnMetaData(
                    columns.size(),
                    false,
                    true,
                    false,
                    false,
                    type.isNullable() ? 1 : 0,
                    true,
                    0,
                    field.getName(),
                    null,
                    null,
                    sqlTypeName.allowsPrec() && false
                        ? type.getPrecision()
                        : -1,
                    sqlTypeName.allowsScale() ? type.getScale() : -1,
                    null,
                    null,
                    sqlTypeName.getJdbcOrdinal(),
                    sqlTypeName.getName(),
                    true,
                    false,
                    false,
                    null));
        }
        return new PrepareResult(
            sql,
            parameters,
            columns,
            (Enumerable) preparedResult.execute());
    }

    private static RelDataType makeStruct(
        RelDataTypeFactory typeFactory,
        RelDataType type)
    {
        if (type.isStruct()) {
            return type;
        }
        return typeFactory.createStructType(
            RelDataTypeFactory.FieldInfoBuilder.of("$0", type));
    }

    private static class OptiqPreparingStmt extends OJPreparingStmt {
        private final RelOptPlanner planner;
        private final RexBuilder rexBuilder;
        private final Schema schema;
        private final Map root;

        public OptiqPreparingStmt(
            RelOptConnection connection,
            RelDataTypeFactory typeFactory,
            Schema schema,
            Map root)
        {
            super(connection);
            this.schema = schema;
            this.root = root;
            planner = new VolcanoPlanner();
            planner.addRelTraitDef(CallingConventionTraitDef.instance);
            RelOptUtil.registerAbstractRels(planner);
            planner.addRule(JavaRules.ENUMERABLE_JOIN_RULE);
            planner.addRule(JavaRules.ENUMERABLE_CALC_RULE);
            planner.addRule(JavaRules.ENUMERABLE_AGGREGATE_RULE);
            planner.addRule(JavaRules.ENUMERABLE_SORT_RULE);
            planner.addRule(JavaRules.ENUMERABLE_UNION_RULE);
            planner.addRule(JavaRules.ENUMERABLE_INTERSECT_RULE);
            planner.addRule(JavaRules.ENUMERABLE_MINUS_RULE);
            planner.addRule(TableAccessRule.instance);

            rexBuilder = new RexBuilder(typeFactory);
        }

        public PreparedResult prepareExpression(
            Expression expression, RelDataType resultType)
        {
            queryString = null;
            Class runtimeContextClass = connection.getClass();
            final Argument [] arguments = {
                new Argument(
                    connectionVariable,
                    runtimeContextClass,
                    connection)
            };
            ClassDeclaration decl = init(arguments);

            final RelOptQuery query = new RelOptQuery(planner);
            final RelOptCluster cluster =
                query.createCluster(
                    env, rexBuilder.getTypeFactory(), rexBuilder);

            RelNode rootRel =
                new LixToRelTranslator(cluster, connection)
                    .translate(expression);

            if (timingTracer != null) {
                timingTracer.traceTime("end sql2rel");
            }

            final RelDataType jdbcType =
                makeStruct(rexBuilder.getTypeFactory(), resultType);
            fieldOrigins = Collections.nCopies(jdbcType.getFieldCount(), null);

            // Structured type flattening, view expansion, and plugging in
            // physical storage.
            rootRel = flattenTypes(rootRel, true);

            rootRel = (RelNode) optimize(resultType, rootRel);
            containsJava = treeContainsJava(rootRel);

            if (timingTracer != null) {
                timingTracer.traceTime("end optimization");
            }

            return implement(
                resultType,
                rootRel,
                SqlKind.SELECT,
                decl,
                arguments);
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
        protected EnumerableRelImplementor getRelImplementor(
            RexBuilder rexBuilder)
        {
            return new EnumerableRelImplementor(rexBuilder);
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

        @Override
        protected PreparedExecution implement(
            RelDataType rowType,
            RelNode rootRel,
            SqlKind sqlKind,
            ClassDeclaration decl,
            Argument[] args)
        {
            RelDataType resultType = rootRel.getRowType();
            boolean isDml = sqlKind.belongsTo(SqlKind.DML);
            javaCompiler = createCompiler();
            EnumerableRelImplementor relImplementor =
                getRelImplementor(rootRel.getCluster().getRexBuilder());
            BlockExpression expr =
                relImplementor.implementRoot((EnumerableRel) rootRel);
            ParameterExpression root0 =
                Expressions.parameter(Map.class, "root0");
            String s = Expressions.toString(
                Blocks.create(
                    Expressions.declare(
                        Modifier.FINAL,
                        OptiqCatalogReader.rootExpression,
                        root0),
                    expr),
                false);
            System.out.println(s);

            final Executable executable;
            try {
                executable = (Executable)
                    ExpressionEvaluator.createFastScriptEvaluator(
                        s, Executable.class, new String[]{"root0"});
            } catch (Exception e) {
                throw Helper.INSTANCE.wrap(
                    "Error while compiling generated Java code:\n" + s, e);
            }

            if (timingTracer != null) {
                timingTracer.traceTime("end codegen");
            }

            if (timingTracer != null) {
                timingTracer.traceTime("end compilation");
            }

            return new PreparedExecution(
                null,
                rootRel,
                resultType,
                isDml,
                mapTableModOp(isDml, sqlKind),
                null)
            {
                public Object execute() {
                    return executable.execute(root);
                }
            };
        }
    }

    private static class Table
        implements SqlValidatorTable, RelOptTable
    {
        private final RelOptSchema schema;
        private final RelDataType rowType;
        private final String[] names;
        private final Expression expression;

        public Table(
            RelOptSchema schema,
            RelDataType rowType,
            String[] names,
            Expression expression)
        {
            this.schema = schema;
            this.rowType = rowType;
            this.names = names;
            this.expression = expression;
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
                cluster, this, connection, expression);
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

        private static final ParameterExpression rootExpression =
            Expressions.variable(Map.class, "root");

        public OptiqCatalogReader(
            Schema schema, RelDataTypeFactory typeFactory)
        {
            super();
            this.schema = schema;
            this.typeFactory = typeFactory;
        }

        public Table getTable(final String[] names) {
            Schema schema2 = schema;
            Expression expression = rootExpression;
            for (int i = 0; i < names.length; i++) {
                final String name = names[i];
                final SchemaObject schemaObject = schema2.get(name);
                final List<Expression> arguments = Collections.emptyList();
                expression = schema2.getExpression(
                    expression,
                    schemaObject,
                    name,
                    arguments);
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
                            toEnumerable(expression));
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

        private Expression toEnumerable(Expression expression) {
            Type type = expression.getType();
            if (Types.isAssignableFrom(Enumerable.class, type)) {
                return expression;
            }
            if (Types.isArray(type)) {
                return Expressions.call(
                    Linq4j.class,
                    "asEnumerable3", // FIXME
                    Collections.singletonList(expression));
            }
            throw new RuntimeException(
                "cannot convert expression [" + expression + "] to enumerable");
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

    private static class LixToRelTranslator {
        final RelOptCluster cluster;
        private final RelOptConnection connection;
        final JavaTypeFactory typeFactory;

        public LixToRelTranslator(
            RelOptCluster cluster,
            RelOptConnection connection)
        {
            this.cluster = cluster;
            this.connection = connection;
            this.typeFactory = (JavaTypeFactory) cluster.getTypeFactory();
        }

        public RelNode translate(Expression expression) {
            if (expression instanceof MethodCallExpression) {
                MethodCallExpression call = (MethodCallExpression) expression;
                if (call.method.equals(METHOD_ENUMERABLE_SELECT)) {
                    RelNode child = translate(call.targetExpression);
                    return new ProjectRel(
                        cluster,
                        child,
                        toRex(
                            child,
                            (FunctionExpression) call.expressions.get(0)),
                        null,
                        ProjectRel.Flags.Boxed);
                }
                if (call.method.equals(METHOD_ENUMERABLE_WHERE)) {
                    RelNode child = translate(call.targetExpression);
                    return new FilterRel(
                        cluster,
                        child,
                        toRex(
                            (FunctionExpression) call.expressions.get(0),
                            child));
                }
                if (call.method.equals(METHOD_ENUMERABLE_ASQUERYABLE)) {
                    return new TableAccessRel(
                        cluster,
                        new Table(
                            null,
                            typeFactory.createJavaType(
                                Types.toClass(
                                    Types.getComponentType(
                                        call.targetExpression.getType()))),
                            new String[0],
                            call.targetExpression),
                        connection);
                }
                throw new UnsupportedOperationException(
                    "unknown method " + call.method);
            }
            throw new UnsupportedOperationException(
                "unknown expression type " + expression.getNodeType());
        }

        private RexNode[] toRex(
            RelNode child, FunctionExpression expression)
        {
            List<RexNode> list = new ArrayList<RexNode>();
            RexBuilder rexBuilder = cluster.getRexBuilder();
            for (RelNode input : new RelNode[]{child}) {
                list.add(rexBuilder.makeRangeReference(input.getRowType()));
            }
            ScalarTranslator translator =
                EmptyScalarTranslator
                    .empty(rexBuilder)
                    .bind(expression.parameterList, list);
            final List<RexNode> rexList = new ArrayList<RexNode>();
            final Expression simple = Blocks.simple(expression.body);
            for (Expression expression1 : fieldExpressions(simple)) {
                rexList.add(translator.toRex(expression1));
            }
            return rexList.toArray(new RexNode[rexList.size()]);
        }

        List<Expression> fieldExpressions(Expression expression) {
            if (expression instanceof NewExpression) {
                // Note: We are assuming that the arguments to the constructor
                // are the same order as the fields of the class.
                return ((NewExpression) expression).arguments;
            }
            throw new RuntimeException(
                "unsupported expression type " + expression);
        }

        private RexNode toRex(
            FunctionExpression expression,
            RelNode... inputs)
        {
            List<RexNode> list = new ArrayList<RexNode>();
            RexBuilder rexBuilder = cluster.getRexBuilder();
            for (RelNode input : inputs) {
                list.add(rexBuilder.makeRangeReference(input.getRowType()));
            }
            return EmptyScalarTranslator.empty(rexBuilder)
                .bind(expression.parameterList, list)
                .toRex(expression.body);
        }
    }

    private interface ScalarTranslator {
        RexNode toRex(BlockExpression expression);
        RexNode toRex(Expression expression);
        ScalarTranslator bind(
            List<ParameterExpression> parameterList, List<RexNode> values);
    }

    private static class EmptyScalarTranslator implements ScalarTranslator {
        private final RexBuilder rexBuilder;

        public EmptyScalarTranslator(RexBuilder rexBuilder) {
            this.rexBuilder = rexBuilder;
        }

        public static ScalarTranslator empty(RexBuilder builder) {
            return new EmptyScalarTranslator(builder);
        }

        public RexNode toRex(BlockExpression expression) {
            return toRex(Blocks.simple(expression));
        }

        public RexNode toRex(Expression expression) {
            switch (expression.getNodeType()) {
            case MemberAccess:
                return rexBuilder.makeFieldAccess(
                    toRex(
                        ((MemberExpression) expression).expression),
                    ((MemberExpression) expression).field.getName());
            case GreaterThan:
                return binary(
                    expression, SqlStdOperatorTable.greaterThanOperator);
            case LessThan:
                return binary(expression, SqlStdOperatorTable.lessThanOperator);
            case Parameter:
                return parameter((ParameterExpression) expression);
            case Call:
                MethodCallExpression call = (MethodCallExpression) expression;
                SqlOperator operator =
                    RexToLixTranslator.JAVA_TO_SQL_METHOD_MAP.get(call.method);
                if (operator != null) {
                    return rexBuilder.makeCall(
                        operator,
                        toRex(
                            Expressions.<Expression>list()
                                .appendIfNotNull(call.targetExpression)
                                .appendAll(call.expressions)));
                }
                throw new RuntimeException(
                    "Could translate call to method " + call.method);
            case Constant:
                final ConstantExpression constant =
                    (ConstantExpression) expression;
                Object value = constant.value;
                if (value instanceof Number) {
                    Number number = (Number) value;
                    if (value instanceof Double || value instanceof Float) {
                        return rexBuilder.makeApproxLiteral(
                            BigDecimal.valueOf(number.doubleValue()));
                    } else if (value instanceof BigDecimal) {
                        return rexBuilder.makeExactLiteral((BigDecimal) value);
                    } else {
                        return rexBuilder.makeExactLiteral(
                            BigDecimal.valueOf(number.longValue()));
                    }
                } else if (value instanceof Boolean) {
                    return rexBuilder.makeLiteral((Boolean) value);
                } else {
                    return rexBuilder.makeLiteral(constant.toString());
                }
            default:
                throw new UnsupportedOperationException(
                    "unknown expression type " + expression.getNodeType() + " "
                    + expression);
            }
        }

        private RexNode binary(Expression expression, SqlBinaryOperator op) {
            BinaryExpression call = (BinaryExpression) expression;
            return rexBuilder.makeCall(
                op, toRex(Arrays.asList(call.expression0, call.expression1)));
        }

        private List<RexNode> toRex(List<Expression> expressions) {
            ArrayList<RexNode> list = new ArrayList<RexNode>();
            for (Expression expression : expressions) {
                list.add(toRex(expression));
            }
            return list;
        }

        public ScalarTranslator bind(
            List<ParameterExpression> parameterList, List<RexNode> values)
        {
            return new LambdaScalarTranslator(
                rexBuilder, parameterList, values);
        }

        public RexNode parameter(ParameterExpression param) {
            throw new RuntimeException("unknown parameter " + param);
        }
    }

    private static class LambdaScalarTranslator extends EmptyScalarTranslator {
        private final List<ParameterExpression> parameterList;
        private final List<RexNode> values;

        public LambdaScalarTranslator(
            RexBuilder rexBuilder,
            List<ParameterExpression> parameterList,
            List<RexNode> values)
        {
            super(rexBuilder);
            this.parameterList = parameterList;
            this.values = values;
        }

        public RexNode parameter(ParameterExpression param) {
            int i = parameterList.indexOf(param);
            if (i >= 0) {
                return values.get(i);
            }
            throw new RuntimeException("unknown parameter " + param);
        }
    }
}

// End OptiqPrepareImpl.java
