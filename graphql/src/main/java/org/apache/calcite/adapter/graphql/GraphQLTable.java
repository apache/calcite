package org.apache.calcite.adapter.graphql;

import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Type;
import java.util.*;
import javax.annotation.Nullable;

import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.schema.*;

/**
 * Table implementation for GraphQL data sources.
 * Maps GraphQL types to SQL types and executes queries with support for ordering and pagination.
 */
public class GraphQLTable extends AbstractTable implements TranslatableTable, QueryableTable {
  protected final GraphQLCalciteSchema schema;
  final GraphQLObjectType objectType;
  @Nullable
  RelDataTypeFactory typeFactory;
  private final String endpoint;
  @Nullable
  private final String name;
  final String selectMany;
  private final Map<String, String> sqlToGraphQLFields = new HashMap<>();
  private final Map<String, String> graphQLToSQLFields = new HashMap<>();

  private static final Logger LOGGER = LogManager.getLogger(GraphQLToEnumerableConverter.class);

  private static final Set<String> RESERVED_WORDS = new HashSet<>(Arrays.asList(
    "type", "timestamp", "date", "time", "interval", "group", "order",
    "by", "desc", "asc", "select", "from", "where", "having", "join",
    "left", "right", "inner", "outer", "cross", "natural", "union",
    "intersect", "except", "case", "when", "then", "else", "end",
    "cast", "as", "between", "and", "or", "not", "null", "true", "false"
  ));

  /**
   * Creates a GraphQLTable.
   *
   * @param schema     The Calcite schema containing this table
   * @param objectType The GraphQL object type representing this table
   * @param graphQL    The GraphQL client instance
   * @param endpoint   The GraphQL endpoint URL
   */
  public GraphQLTable(GraphQLCalciteSchema schema,
      GraphQLObjectType objectType,
      GraphQL graphQL,
      String endpoint) {
    this.schema = schema;
    this.objectType = objectType;
    this.selectMany = this.findSelectMany(graphQL, objectType);
    this.endpoint = endpoint;
    this.name = objectType.getName();
  }

  private void mapField(String graphQLField) {
    String sqlField = RESERVED_WORDS.contains(graphQLField.toLowerCase())
        ? graphQLField + "_"
        : graphQLField;

    sqlToGraphQLFields.put(sqlField, graphQLField);
    graphQLToSQLFields.put(graphQLField, sqlField);
  }

  public String getSQLFieldName(String graphQLField) {
    return graphQLToSQLFields.get(graphQLField);
  }

  public String getGraphQLFieldName(String sqlField) {
    return sqlToGraphQLFields.get(sqlField);
  }

  @Nullable
  public String getName() {
    return name;
  }

  public String getSelectMany() {
    return selectMany;
  }

  private String findSelectMany(GraphQL graphQL, GraphQLObjectType objectType) {
    GraphQLObjectType queryType = graphQL.getGraphQLSchema().getQueryType();
    if (queryType == null) {
      throw new IllegalStateException("Schema does not have a query type");
    }

    for (GraphQLFieldDefinition field : queryType.getFieldDefinitions()) {
      GraphQLOutputType fieldType = field.getType();
      GraphQLType unwrappedType = unwrapType(fieldType);

      if (unwrappedType instanceof GraphQLList) {
        GraphQLType listType = unwrapType(((GraphQLList) unwrappedType).getWrappedType());
        if (listType.equals(objectType)) {
          return field.getName();
        }
      }
    }

    return null;
  }

  /**
   * Executes a GraphQL query with the specified fields list.
   * The query string should include any ordering, offset, and limit clauses.
   *
   * @param gqlQuery Complete GraphQL query string
   * @return List of Object arrays containing the query results
   * @throws RuntimeException if there's an error executing the query
   */
  public Enumerable<Object> query(String gqlQuery) {
    return new AbstractEnumerable<Object>() {
      @Override
      public Enumerator<Object> enumerator() {
        try {
          ExecutionResult result = new GraphQLExecutor(gqlQuery, endpoint, schema).executeQuery();
          return new GraphQLEnumerator(result.getData());
        } catch (Exception e) {
          throw new RuntimeException("Error executing GraphQL query: " + gqlQuery, e);
        }
      }
    };
  }

  /**
   * Retrieves the expression for the specified table and class from the given schema.
   *
   * @param schema    The parent schema
   * @param tableName The name of the table
   * @param clazz     The class representing the table
   * @return Expression representing the table
   */
  @Override
  public Expression getExpression(SchemaPlus schema, String tableName,
      Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  /**
   * Convert this table into a {@link Queryable} that can evaluate queries against
   * a specific data source.
   *
   * @param queryProvider The query provider used to create and execute queries
   * @param schema        The schema containing this table
   * @param tableName     The name of the table
   * @param <T>           Element type of the queryable
   * @return A {@link Queryable} representing this table
   */
  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }

  /**
   * Retrieves the type of elements in the table.
   *
   * @return Type representing the elements in the table
   */
  @Override
  public Type getElementType() {
    return Object[].class;
  }

  /**
   * Converts the GraphQLTable to a RelNode.
   *
   * @param context     The context for converting the table to relational expression
   * @param relOptTable The relational optimization table
   * @return The RelNode representing the GraphQLTable
   */
  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    LOGGER.debug("Converting GraphQLTable to RelNode");
    final RelOptCluster cluster = context.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(GraphQLRel.CONVENTION);
    return new GraphQLTableScan(
        cluster,
        traitSet,
        relOptTable,
        this,
        relOptTable.getRowType(),
        null);
  }

  /**
   * Retrieves the row type of this GraphQLTable based on the provided RelDataTypeFactory.
   * This method iterates through the field definitions of the GraphQL object type,
   * converts each GraphQL field type to a corresponding SQL type, and adds them to the builder.
   *
   * @param typeFactory The factory used to create RelDataType instances
   * @return The resulting RelDataType representing the row type of this table
   */
  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    this.typeFactory = typeFactory;
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    List<GraphQLFieldDefinition> fieldDefinitions = objectType.getFieldDefinitions();

    for (GraphQLFieldDefinition field : fieldDefinitions) {
      GraphQLType fieldType = unwrapType(field.getType());

      if (fieldType instanceof GraphQLNonNull) {
        fieldType = ((GraphQLNonNull) fieldType).getWrappedType();
      }

      if (fieldType instanceof GraphQLList) {
        fieldType = ((GraphQLList) fieldType).getWrappedType();
      }

      // Skip if the field is not a scalar type
      if (!(fieldType instanceof GraphQLScalarType || fieldType instanceof GraphQLEnumType)) {
        continue;
      }

      String graphQLField = field.getName();
      mapField(graphQLField);
      String sqlField = graphQLToSQLFields.get(graphQLField);

      RelDataType sqlType = convertGraphQLTypeToRelDataType(fieldType, typeFactory);
      builder.add(sqlField, sqlType);
    }

    return builder.build();
  }

  /**
   * Unwraps a GraphQL type from any NonNull wrapper.
   */
  private GraphQLType unwrapType(GraphQLType type) {
    if (type instanceof GraphQLNonNull) {
      return unwrapType(((GraphQLNonNull) type).getWrappedType());
    }
    return type;
  }

  /**
   * Converts a GraphQL type to the corresponding Calcite SQL type.
   */
  private RelDataType convertGraphQLTypeToRelDataType(GraphQLType type,
      RelDataTypeFactory typeFactory) {
    if (type instanceof GraphQLNonNull) {
      return typeFactory.createTypeWithNullability(
          convertGraphQLTypeToRelDataType(
              ((GraphQLNonNull) type).getWrappedType(),
              typeFactory
          ),
          false
      );
    } else if (type instanceof GraphQLList) {
      RelDataType elementType =
          convertGraphQLTypeToRelDataType(
              ((GraphQLList) type).getWrappedType(),
              typeFactory
          );
      return typeFactory.createArrayType(elementType, -1);
    } else if (type instanceof GraphQLObjectType) {
      return typeFactory.createSqlType(SqlTypeName.MAP);
    } else if (type instanceof GraphQLEnumType) {
      return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    } else if (type instanceof GraphQLScalarType) {
      return mapScalarType((GraphQLScalarType) type, typeFactory);
    }
    return typeFactory.createSqlType(SqlTypeName.VARCHAR);
  }

  /**
   * Converts a string timestamp to a SQL timestamp format
   */
  private RelDataType createTimestampType(RelDataTypeFactory typeFactory, boolean withTimeZone) {
    return typeFactory.createSqlType(
        withTimeZone ? SqlTypeName.TIMESTAMP_TZ : SqlTypeName.TIMESTAMP,
        9  // Set precision to 9 for microseconds
    );
  }

  /**
   * Maps GraphQL scalar types to SQL types.
   */
  private RelDataType mapScalarType(GraphQLScalarType scalarType,
      RelDataTypeFactory typeFactory) {
    String name = scalarType.getName();
    String trimmedName = name.replaceAll("(_\\d+)$", "");
    switch (trimmedName.toLowerCase()) {
    case "text":
    case "id":
    case "string":
    case "varchar":
      return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    case "int":
    case "int4":
    case "integer":
      return typeFactory.createSqlType(SqlTypeName.INTEGER);
    case "float":
    case "float4":
      return typeFactory.createSqlType(SqlTypeName.REAL);
    case "double":
    case "float8":
      return typeFactory.createSqlType(SqlTypeName.DOUBLE);
    case "bool":
    case "boolean":
      return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    case "list":
      return typeFactory.createSqlType(SqlTypeName.ARRAY);
    case "map":
    case "object":
      return typeFactory.createSqlType(SqlTypeName.MAP);
    case "date":
      return typeFactory.createSqlType(SqlTypeName.DATE);
    case "timestamp":
    case "datetime":
    case "timestamptz":
      return createTimestampType(typeFactory, false);
    case "bigdecimal":
    case "decimal":
      return typeFactory.createSqlType(SqlTypeName.DECIMAL);
    case "long":
    case "int8":
    case "bigint":
    case "biginteger":
      return typeFactory.createSqlType(SqlTypeName.BIGINT);
    case "byte":
      return typeFactory.createSqlType(SqlTypeName.TINYINT);
    case "short":
    case "int2":
      return typeFactory.createSqlType(SqlTypeName.SMALLINT);
    case "binary":
    case "bytea":
      return typeFactory.createSqlType(SqlTypeName.BINARY);
    case "char":
      return typeFactory.createSqlType(SqlTypeName.CHAR);
    case "time":
      return typeFactory.createSqlType(SqlTypeName.TIME);
    case "interval":
      return typeFactory.createSqlType(SqlTypeName.INTERVAL_YEAR_MONTH);
    default:
      return typeFactory.createSqlType(SqlTypeName.ANY);
    }
  }

  @Override
  public String toString() {
    return "GraphQLTableImpl:" + (getName() != null ? getName() : "unknown");
  }
}
