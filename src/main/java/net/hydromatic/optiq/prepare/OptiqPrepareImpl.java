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
import net.hydromatic.linq4j.function.Function0;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.Helper;
import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.rules.java.*;
import net.hydromatic.optiq.runtime.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.volcano.VolcanoPlanner;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.ChainedSqlOperatorTable;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql2rel.SqlToRelConverter;
import org.eigenbase.util.Bug;
import org.eigenbase.util.Pair;

import org.codehaus.janino.*;
import org.codehaus.janino.Scanner;

import java.io.StringReader;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;

/**
 * Shit just got real.
 *
 * <p>This class is public so that projects that create their own JDBC driver
 * and server can fine-tune preferences. However, this class and its methods are
 * subject to change without notice.</p>
 */
public class OptiqPrepareImpl implements OptiqPrepare {

  public static final boolean DEBUG =
      "true".equals(System.getProperties().getProperty("optiq.debug"));

  public ParseResult parse(
      Context context, String sql) {
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    OptiqCatalogReader catalogReader =
        new OptiqCatalogReader(
            context.getRootSchema(),
            context.getDefaultSchemaPath(),
            typeFactory);
    SqlParser parser = new SqlParser(sql);
    SqlNode sqlNode;
    try {
      sqlNode = parser.parseStmt();
    } catch (SqlParseException e) {
      throw new RuntimeException("parse failed", e);
    }
    final SqlValidator validator =
        new OptiqSqlValidator(
            SqlStdOperatorTable.instance(), catalogReader, typeFactory);
    SqlNode sqlNode1 = validator.validate(sqlNode);
    return new ParseResult(
        sql, sqlNode1, validator.getValidatedNodeType(sqlNode1));
  }

  /** Creates a collection of planner factories.
   *
   * <p>The collection must have at least one factory, and each factory must
   * create a planner. If the collection has more than one planner, Optiq will
   * try each planner in turn.</p>
   *
   * <p>One of the things you can do with this mechanism is to try a simpler,
   * faster, planner with a smaller rule set first, then fall back to a more
   * complex planner for complex and costly queries.</p>
   *
   * <p>The default implementation returns a factory that calls
   * {@link #createPlanner()}.</p> */
  protected List<Function0<RelOptPlanner>> createPlannerFactories() {
    return Collections.<Function0<RelOptPlanner>>singletonList(
        new Function0<RelOptPlanner>() {
          public RelOptPlanner apply() {
            return createPlanner();
          }
        }
    );
  }

  /** Creates a query planner and initializes it with a default set of
   * rules. */
  protected RelOptPlanner createPlanner() {
    final VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.instance);
    if (Bug.TodoFixed)
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    if (Bug.TodoFixed)
    planner.registerAbstractRelationalRules();
    RelOptUtil.registerAbstractRels(planner);
    planner.addRule(JavaRules.ENUMERABLE_JOIN_RULE);
    planner.addRule(JavaRules.ENUMERABLE_CALC_RULE);
    planner.addRule(JavaRules.ENUMERABLE_AGGREGATE_RULE);
    planner.addRule(JavaRules.ENUMERABLE_SORT_RULE);
    planner.addRule(JavaRules.ENUMERABLE_UNION_RULE);
    planner.addRule(JavaRules.ENUMERABLE_INTERSECT_RULE);
    planner.addRule(JavaRules.ENUMERABLE_MINUS_RULE);
    planner.addRule(JavaRules.ENUMERABLE_TABLE_MODIFICATION_RULE);
    planner.addRule(JavaRules.ENUMERABLE_VALUES_RULE);
    planner.addRule(JavaRules.ENUMERABLE_ONE_ROW_RULE);
    planner.addRule(JavaRules.ENUMERABLE_CUSTOM_FROM_ARRAY_RULE);
    planner.addRule(JavaRules.ENUMERABLE_ARRAY_FROM_CUSTOM_RULE);
    planner.addRule(JavaRules.EnumerableCustomCalcRule.INSTANCE);
    planner.addRule(TableAccessRule.instance);
    planner.addRule(PushFilterPastProjectRule.instance);
    planner.addRule(PushFilterPastJoinRule.instance);
    planner.addRule(RemoveDistinctAggregateRule.instance);
    planner.addRule(ReduceAggregatesRule.instance);
    planner.addRule(SwapJoinRule.instance);
    return planner;
  }

  public <T> PrepareResult<T> prepareQueryable(
      Context context,
      Queryable<T> queryable) {
    return prepare_(
        context, null, queryable, queryable.getElementType(), -1);
  }

  public <T> PrepareResult<T> prepareSql(
      Context context,
      String sql,
      Queryable<T> expression,
      Type elementType,
      int maxRowCount) {
    return prepare_(context, sql, expression, elementType, maxRowCount);
  }

  <T> PrepareResult<T> prepare_(
      Context context,
      String sql,
      Queryable<T> queryable,
      Type elementType,
      int maxRowCount) {
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    OptiqCatalogReader catalogReader =
        new OptiqCatalogReader(
            context.getRootSchema(),
            context.getDefaultSchemaPath(),
            typeFactory);
    final List<Function0<RelOptPlanner>> plannerFactories =
        createPlannerFactories();
    if (plannerFactories.isEmpty()) {
      throw new AssertionError("no planner factories");
    }
    RuntimeException exception = new RuntimeException();
    for (Function0<RelOptPlanner> plannerFactory : plannerFactories) {
      final RelOptPlanner planner = plannerFactory.apply();
      if (planner == null) {
        throw new AssertionError("factory returned null planner");
      }
      try {
        return prepare2_(
            context, sql, queryable, elementType, maxRowCount,
            catalogReader, planner);
      } catch (RelOptPlanner.CannotPlanException e) {
        exception = e;
      }
    }
    throw exception;
  }

  <T> PrepareResult<T> prepare2_(
      Context context,
      String sql,
      Queryable<T> queryable,
      Type elementType,
      int maxRowCount,
      OptiqCatalogReader catalogReader,
      RelOptPlanner planner) {
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    final EnumerableConvention convention;
    if (elementType == Object[].class) {
      convention = EnumerableConvention.ARRAY;
    } else {
      convention = EnumerableConvention.CUSTOM;
    }
    final OptiqPreparingStmt preparingStmt =
        new OptiqPreparingStmt(
            catalogReader,
            typeFactory,
            context.getRootSchema(),
            convention,
            planner);

    final RelDataType x;
    final Prepare.PreparedResult preparedResult;
    if (sql != null) {
      assert queryable == null;
      SqlParser parser = new SqlParser(sql);
      SqlNode sqlNode;
      try {
        sqlNode = parser.parseStmt();
      } catch (SqlParseException e) {
        throw new RuntimeException("parse failed", e);
      }
      final Schema rootSchema = context.getRootSchema();
      final ChainedSqlOperatorTable opTab =
          new ChainedSqlOperatorTable(
              Arrays.<SqlOperatorTable>asList(
                  SqlStdOperatorTable.instance(),
                  new OptiqSqlOperatorTable(rootSchema, typeFactory)));
      final SqlValidator validator =
          new OptiqSqlValidator(opTab, catalogReader, typeFactory);
      preparedResult = preparingStmt.prepareSql(
          sqlNode, Object.class, validator, true);
      switch (sqlNode.getKind()) {
      case INSERT:
      case EXPLAIN:
        // FIXME: getValidatedNodeType is wrong for DML
        x = RelOptUtil.createDmlRowType(sqlNode.getKind(), typeFactory);
        break;
      default:
        x = validator.getValidatedNodeType(sqlNode);
      }
    } else {
      assert queryable != null;
      x = context.getTypeFactory().createType(elementType);
      preparedResult =
          preparingStmt.prepareQueryable(queryable, x);
    }

    // TODO: parameters
    final List<Parameter> parameters = Collections.emptyList();
    // TODO: column meta data
    final List<ColumnMetaData> columns =
        new ArrayList<ColumnMetaData>();
    RelDataType jdbcType = makeStruct(typeFactory, x);
    final List<List<String>> originList = preparedResult.getFieldOrigins();
    for (Ord<RelDataTypeField> pair : Ord.zip(jdbcType.getFields())) {
      final RelDataTypeField field = pair.e;
      RelDataType type = field.getType();
      List<String> origins = originList.get(pair.i);
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
              type.getPrecision(),
              field.getName(),
              origins == null ? null : origins.get(2),
              origins == null ? null : origins.get(0),
              type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
                  ? 0
                  : type.getPrecision(),
              type.getScale() == RelDataType.SCALE_NOT_SPECIFIED
                  ? 0
                  : type.getScale(),
              origins == null ? null : origins.get(1),
              null,
              sqlTypeName.getJdbcOrdinal(),
              sqlTypeName.getName(),
              true,
              false,
              false,
              null,
              (Class) typeFactory.getJavaClass(
                  x.isStruct()
                      ? x.getFieldList().get(pair.i).getType()
                      : type)));
    }
    Enumerable<T> enumerable =
        (Enumerable<T>) preparedResult.execute();
    if (maxRowCount >= 0) {
      // Apply limit. In JDBC 0 means "no limit". But for us, -1 means
      // "no limit", and 0 is a valid limit.
      enumerable = enumerable.take(maxRowCount);
    }
    Class resultClazz = null;
    if (preparedResult instanceof Typed) {
      resultClazz = (Class) ((Typed) preparedResult).getElementType();
    }
    return new PrepareResult<T>(
        sql,
        parameters,
        columns,
        enumerable,
        resultClazz);
  }

  private static RelDataType makeStruct(
      RelDataTypeFactory typeFactory,
      RelDataType type) {
    if (type.isStruct()) {
      return type;
    }
    return typeFactory.createStructType(
        RelDataTypeFactory.FieldInfoBuilder.of("$0", type));
  }

  private static class OptiqPreparingStmt extends Prepare {
    private final RelOptPlanner planner;
    private final RexBuilder rexBuilder;
    private final Schema schema;
    private int expansionDepth;
    private SqlValidator sqlValidator;

    public OptiqPreparingStmt(
        CatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        Schema schema,
        Convention resultConvention,
        RelOptPlanner planner) {
      super(catalogReader, resultConvention);
      this.schema = schema;
      this.planner = planner;
      this.rexBuilder = new RexBuilder(typeFactory);
    }

    @Override
    protected void init(Class runtimeContextClass) {
    }

    public PreparedResult prepareQueryable(
        Queryable queryable,
        RelDataType resultType) {
      queryString = null;
      Class runtimeContextClass = Object.class;
      init(runtimeContextClass);

      final RelOptQuery query = new RelOptQuery(planner);
      final RelTraitSet emptyTraitSet = RelTraitSet.createEmpty();
      final RelOptCluster cluster =
          query.createCluster(
              rexBuilder.getTypeFactory(), rexBuilder, emptyTraitSet);

      RelNode rootRel =
          new LixToRelTranslator(cluster, OptiqPreparingStmt.this)
              .translate(queryable);

      if (timingTracer != null) {
        timingTracer.traceTime("end sql2rel");
      }

      final RelDataType jdbcType =
          makeStruct(rexBuilder.getTypeFactory(), resultType);
      fieldOrigins = Collections.nCopies(jdbcType.getFieldCount(), null);

      // Structured type flattening, view expansion, and plugging in
      // physical storage.
      rootRel = flattenTypes(rootRel, true);

      // Trim unused fields.
      rootRel = trimUnusedFields(rootRel);

      rootRel = optimize(resultType, rootRel);

      if (timingTracer != null) {
        timingTracer.traceTime("end optimization");
      }

      return implement(
          resultType,
          rootRel,
          SqlKind.SELECT);
    }

    @Override
    protected SqlToRelConverter getSqlToRelConverter(
        SqlValidator validator,
        CatalogReader catalogReader) {
      SqlToRelConverter sqlToRelConverter =
          new SqlToRelConverter(
              this, validator, catalogReader, planner, rexBuilder);
      sqlToRelConverter.setTrimUnusedFields(false);
      return sqlToRelConverter;
    }

    @Override
    protected EnumerableRelImplementor getRelImplementor(
        RexBuilder rexBuilder) {
      return new EnumerableRelImplementor(rexBuilder);
    }

    @Override
    protected boolean shouldAlwaysWriteJavaFile() {
      return false;
    }

    @Override
    public RelNode flattenTypes(
        RelNode rootRel,
        boolean restructure) {
      return rootRel;
    }

    @Override
    protected RelNode decorrelate(SqlNode query, RelNode rootRel) {
      return rootRel;
    }

    @Override
    public RelNode expandView(
        RelDataType rowType,
        String queryString,
        List<String> schemaPath) {
      expansionDepth++;

      SqlParser parser = new SqlParser(queryString);
      SqlNode sqlNode;
      try {
        sqlNode = parser.parseQuery();
      } catch (SqlParseException e) {
        throw new RuntimeException("parse failed", e);
      }
      // View may have different schema path than current connection.
      final OptiqCatalogReader catalogReader =
          new OptiqCatalogReader(
              ((OptiqCatalogReader) this.catalogReader).rootSchema,
              schemaPath,
              ((OptiqCatalogReader) this.catalogReader).typeFactory);
      SqlValidator validator = createSqlValidator(catalogReader);
      SqlNode sqlNode1 = validator.validate(sqlNode);

      SqlToRelConverter sqlToRelConverter =
          getSqlToRelConverter(validator, catalogReader);
      RelNode relNode =
          sqlToRelConverter.convertQuery(sqlNode1, true, false);

      --expansionDepth;
      return relNode;
    }

    private SqlValidatorImpl createSqlValidator(CatalogReader catalogReader) {
      return new SqlValidatorImpl(
          SqlStdOperatorTable.instance(), catalogReader,
          rexBuilder.getTypeFactory(), SqlConformance.Default) { };
    }

    @Override
    protected SqlValidator getSqlValidator() {
      if (sqlValidator == null) {
        sqlValidator = createSqlValidator(catalogReader);
      }
      return sqlValidator;
    }

    @Override
    protected PreparedResult createPreparedExplanation(
        RelDataType resultType,
        RelNode rootRel,
        boolean explainAsXml,
        SqlExplainLevel detailLevel) {
      return new OptiqPreparedExplain(
          resultType, rootRel, explainAsXml, detailLevel);
    }

    @Override
    protected PreparedResult implement(
        RelDataType rowType,
        RelNode rootRel,
        SqlKind sqlKind) {
      RelDataType resultType = rootRel.getRowType();
      boolean isDml = sqlKind.belongsTo(SqlKind.DML);
      EnumerableRelImplementor relImplementor =
          getRelImplementor(rootRel.getCluster().getRexBuilder());
      ClassDeclaration expr =
          relImplementor.implementRoot((EnumerableRel) rootRel);
      String s =
          Expressions.toString(expr.memberDeclarations, "\n", false);

      if (DEBUG) {
        System.out.println();
        System.out.println(s);
      }

      Hook.JAVA_PLAN.run(s);

      final Executable executable;
      try {
        executable = (Executable)
            ClassBodyEvaluator.createFastClassBodyEvaluator(
                new Scanner(null, new StringReader(s)),
                expr.name,
                Utilities.class,
                new Class[]{Executable.class, Typed.class},
                getClass().getClassLoader());
      } catch (Exception e) {
        throw Helper.INSTANCE.wrap(
            "Error while compiling generated Java code:\n"
                + s,
            e);
      }

      if (timingTracer != null) {
        timingTracer.traceTime("end codegen");
      }

      if (timingTracer != null) {
        timingTracer.traceTime("end compilation");
      }

      return new PreparedResultImpl(
          resultType,
          fieldOrigins,
          rootRel,
          mapTableModOp(isDml, sqlKind),
          isDml) {
        public String getCode() {
          throw new UnsupportedOperationException();
        }

        @Override
        public Object execute() {
          return executable.execute(schema);
        }

        @Override
        public Type getElementType() {
          return ((Typed) executable).getElementType();
        }
      };
    }
  }

  private static class OptiqPreparedExplain extends Prepare.PreparedExplain {
    public OptiqPreparedExplain(
        RelDataType resultType,
        RelNode rootRel,
        boolean explainAsXml,
        SqlExplainLevel detailLevel) {
      super(resultType, rootRel, explainAsXml, detailLevel);
    }

    @Override
    public Object execute() {
      final String explanation = getCode();
      return Linq4j.singletonEnumerable(explanation);
    }
  }

  static class RelOptTableImpl implements Prepare.PreparingTable {
    private final RelOptSchema schema;
    private final RelDataType rowType;
    private final String[] names;
    private final Table table;
    private final Expression expression;

    RelOptTableImpl(
        RelOptSchema schema,
        RelDataType rowType,
        String[] names,
        Table table) {
      this(schema, rowType, names, table, table.getExpression());
    }

    RelOptTableImpl(
        RelOptSchema schema,
        RelDataType rowType,
        String[] names,
        Expression expression) {
      this(schema, rowType, names, null, expression);
    }

    private RelOptTableImpl(
        RelOptSchema schema,
        RelDataType rowType,
        String[] names,
        Table table,
        Expression expression) {
      this.schema = schema;
      this.rowType = rowType;
      this.names = names;
      this.table = table;
      this.expression = expression;
      assert expression != null : "table may be null; expr may not";
    }

    public <T> T unwrap(
        Class<T> clazz) {
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      if (clazz.isInstance(table)) {
        return clazz.cast(table);
      }
      return null;
    }

    public double getRowCount() {
      if (table != null) {
        final Double rowCount = table.getStatistic().getRowCount();
        if (rowCount != null) {
          return rowCount;
        }
      }
      return 100d;
    }

    public RelOptSchema getRelOptSchema() {
      return schema;
    }

    public RelNode toRel(ToRelContext context) {
      if (table instanceof TranslatableTable) {
        return ((TranslatableTable) table).toRel(context, this);
      }
      RelOptCluster cluster = context.getCluster();
      EnumerableConvention convention = EnumerableConvention.CUSTOM;
      Class elementType = Object[].class;
      if (table != null && table.getElementType() instanceof Class) {
        elementType = (Class) table.getElementType();
        if (Object[].class.isAssignableFrom(elementType)) {
          convention = EnumerableConvention.ARRAY;
        } else {
          convention = EnumerableConvention.CUSTOM;
        }
      }
      return new JavaRules.EnumerableTableAccessRel(
          cluster, cluster.traitSetOf(convention),
          this, expression, elementType);
    }

    public List<RelCollation> getCollationList() {
      return Collections.emptyList();
    }

    public boolean isKey(BitSet columns) {
      return table.getStatistic().isKey(columns);
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
      return SqlAccessType.ALL;
    }
  }

  private static class OptiqCatalogReader
      implements Prepare.CatalogReader {
    private final Schema rootSchema;
    private final JavaTypeFactory typeFactory;
    private final List<String> defaultSchema;

    public OptiqCatalogReader(
        Schema rootSchema,
        List<String> defaultSchema,
        JavaTypeFactory typeFactory) {
      super();
      assert rootSchema != defaultSchema;
      this.rootSchema = rootSchema;
      this.defaultSchema = defaultSchema;
      this.typeFactory = typeFactory;
    }

    public RelOptTableImpl getTable(final String[] names) {
      // First look in the default schema, if any.
      if (defaultSchema != null) {
        RelOptTableImpl table = getTableFrom(names, defaultSchema);
        if (table != null) {
          return table;
        }
      }
      // If not found, look in the root schema
      return getTableFrom(names, Collections.<String>emptyList());
    }

    private RelOptTableImpl getTableFrom(
        String[] names,
        List<String> schemaNames) {
      List<Pair<String, Object>> pairs =
          new ArrayList<Pair<String, Object>>();
      Schema schema = rootSchema;
      for (String schemaName : schemaNames) {
        schema = schema.getSubSchema(schemaName);
        if (schema == null) {
          return null;
        }
        pairs.add(Pair.<String, Object>of(schemaName, schema));
      }
      for (int i = 0; i < names.length; i++) {
        final String name = names[i];
        Schema subSchema = schema.getSubSchema(name);
        if (subSchema != null) {
          pairs.add(Pair.<String, Object>of(name, subSchema));
          schema = subSchema;
          continue;
        }
        final Table table = schema.getTable(name, Object.class);
        if (table != null) {
          pairs.add(Pair.<String, Object>of(name, table));
          if (i != names.length - 1) {
            // not enough objects to match all names
            return null;
          }
          return new RelOptTableImpl(
              this,
              table.getRowType(),
              Pair.left(pairs).toArray(new String[pairs.size()]),
              table);
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

    public RelOptTableImpl getTableForMember(String[] names) {
      return getTable(names);
    }

    public RelDataTypeFactory getTypeFactory() {
      return typeFactory;
    }

    public void registerRules(RelOptPlanner planner) throws Exception {
    }
  }

  interface ScalarTranslator {
    RexNode toRex(BlockExpression expression);
    List<RexNode> toRexList(BlockExpression expression);
    RexNode toRex(Expression expression);
    ScalarTranslator bind(
        List<ParameterExpression> parameterList, List<RexNode> values);
  }

  static class EmptyScalarTranslator implements ScalarTranslator {
    private final RexBuilder rexBuilder;

    public EmptyScalarTranslator(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    public static ScalarTranslator empty(RexBuilder builder) {
      return new EmptyScalarTranslator(builder);
    }

    public List<RexNode> toRexList(BlockExpression expression) {
      final List<Expression> simpleList = simpleList(expression);
      final List<RexNode> list = new ArrayList<RexNode>();
      for (Expression expression1 : simpleList) {
        list.add(toRex(expression1));
      }
      return list;
    }

    public RexNode toRex(BlockExpression expression) {
      return toRex(Blocks.simple(expression));
    }

    private static List<Expression> simpleList(BlockExpression expression) {
      Expression simple = Blocks.simple(expression);
      if (simple instanceof NewExpression) {
        NewExpression newExpression = (NewExpression) simple;
        return newExpression.arguments;
      } else {
        return Collections.singletonList(simple);
      }
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
        List<ParameterExpression> parameterList, List<RexNode> values) {
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
        List<RexNode> values) {
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

  private static class OptiqSqlOperatorTable implements SqlOperatorTable {
    private final Schema rootSchema;
    private final JavaTypeFactory typeFactory;

    public OptiqSqlOperatorTable(
        Schema rootSchema,
        JavaTypeFactory typeFactory) {
      this.rootSchema = rootSchema;
      this.typeFactory = typeFactory;
    }

    public List<SqlOperator> lookupOperatorOverloads(
        SqlIdentifier opName,
        SqlFunctionCategory category,
        SqlSyntax syntax) {
      if (syntax != SqlSyntax.Function) {
        return Collections.emptyList();
      }
      // FIXME: ignoring prefix of opName
      String name = opName.names[opName.names.length - 1];
      List<TableFunction> tableFunctions =
          rootSchema.getTableFunctions(name);
      if (tableFunctions.isEmpty()) {
        return Collections.emptyList();
      }
      return toOps(name, tableFunctions);
    }

    private List<SqlOperator> toOps(
        final String name,
        final List<TableFunction> tableFunctions) {
      return new AbstractList<SqlOperator>() {
        public SqlOperator get(int index) {
          return toOp(name, tableFunctions.get(index));
        }

        public int size() {
          return tableFunctions.size();
        }
      };
    }

    private SqlOperator toOp(String name, TableFunction fun) {
      List<RelDataType> argTypes = new ArrayList<RelDataType>();
      List<SqlTypeFamily> typeFamilies = new ArrayList<SqlTypeFamily>();
      for (net.hydromatic.optiq.Parameter o
          : (List<net.hydromatic.optiq.Parameter>) fun.getParameters()) {
        argTypes.add(o.getType());
        typeFamilies.add(SqlTypeFamily.ANY);
      }
      return new SqlFunction(
          name,
          SqlKind.OTHER_FUNCTION,
          new ExplicitReturnTypeInference(
              typeFactory.createType(fun.getElementType())),
          new ExplicitOperandTypeInference(
              argTypes.toArray(new RelDataType[argTypes.size()])),
          SqlTypeStrategies.family(
              typeFamilies.toArray(
                  new SqlTypeFamily[typeFamilies.size()])),
          null);
    }

    public List<SqlOperator> getOperatorList() {
      return null;
    }
  }

  /** Validator. */
  private static class OptiqSqlValidator extends SqlValidatorImpl {
    public OptiqSqlValidator(
        SqlOperatorTable opTab,
        OptiqCatalogReader catalogReader,
        JavaTypeFactory typeFactory) {
      super(opTab, catalogReader, typeFactory, SqlConformance.Default);
    }

    @Override
    protected RelDataType getLogicalSourceRowType(
        RelDataType sourceRowType, SqlInsert insert) {
      return ((JavaTypeFactory) typeFactory).toSql(sourceRowType);
    }

    @Override
    protected RelDataType getLogicalTargetRowType(
        RelDataType targetRowType, SqlInsert insert) {
      return ((JavaTypeFactory) typeFactory).toSql(targetRowType);
    }
  }
}

// End OptiqPrepareImpl.java
