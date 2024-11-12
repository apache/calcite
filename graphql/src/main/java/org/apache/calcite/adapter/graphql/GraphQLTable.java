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
import java.util.List;
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
  @Nullable RelDataTypeFactory typeFactory;
  private final String endpoint;
  @Nullable private final String name;
  final String selectMany;

  private static final Logger LOGGER = LogManager.getLogger(GraphQLToEnumerableConverter.class);

  /**
   * Creates a GraphQLTable.
   *
   * @param schema The Calcite schema containing this table
   * @param objectType The GraphQL object type representing this table
   * @param graphQL The GraphQL client instance
   * @param endpoint The GraphQL endpoint URL
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
   * @param schema The parent schema
   * @param tableName The name of the table
   * @param clazz The class representing the table
   * @return Expression representing the table
   */
  @Override public Expression getExpression(SchemaPlus schema, String tableName,
      Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  /**
   * Convert this table into a {@link Queryable} that can evaluate queries against
   * a specific data source.
   *
   * @param queryProvider The query provider used to create and execute queries
   * @param schema The schema containing this table
   * @param tableName The name of the table
   * @param <T> Element type of the queryable
   * @return A {@link Queryable} representing this table
   */
  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }

  /**
   * Retrieves the type of elements in the table.
   *
   * @return Type representing the elements in the table
   */
  @Override public Type getElementType() {
    return Object[].class;
  }

  /**
   * Converts the GraphQLTable to a RelNode.
   *
   * @param context The context for converting the table to relational expression
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
        relOptTable.getRowType(), // Pass the table's row type
        null); // No projections yet at table scan level
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

      // Skip if the field is a list of non-scalar types or a map/object type
      if (fieldType instanceof GraphQLList
          && !(((GraphQLList) fieldType).getWrappedType() instanceof GraphQLScalarType)
          || fieldType instanceof GraphQLObjectType) {
        continue;
      }

      RelDataType sqlType = convertGraphQLTypeToRelDataType(field.getType(), typeFactory);
      builder.add(field.getName(), sqlType);
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
    }
    else if (type instanceof GraphQLList) {
      RelDataType elementType =
          convertGraphQLTypeToRelDataType(
              ((GraphQLList) type).getWrappedType(),
              typeFactory
          );
      return typeFactory.createArrayType(elementType, -1);
    }
    else if (type instanceof GraphQLObjectType) {
      return typeFactory.createSqlType(SqlTypeName.MAP);
    }
    else if (type instanceof GraphQLScalarType) {
      return mapScalarType((GraphQLScalarType) type, typeFactory);
    }
    return typeFactory.createSqlType(SqlTypeName.VARCHAR);
  }

  /**
   * Maps GraphQL scalar types to SQL types.
   */
  private RelDataType mapScalarType(GraphQLScalarType scalarType,
      RelDataTypeFactory typeFactory) {
    String name = scalarType.getName();
    String trimmedName = name.replaceAll("\\d+$", "");
    switch (trimmedName) {
    case "Int":
    case "Integer":
      return typeFactory.createSqlType(SqlTypeName.INTEGER);
    case "Float":
    case "Double":
      return typeFactory.createSqlType(SqlTypeName.FLOAT);
    case "ID":
    case "String":
    case "Varchar":
      return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    case "Boolean":
      return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    case "List":
      return typeFactory.createSqlType(SqlTypeName.ARRAY);
    case "Map":
    case "Object":
      return typeFactory.createSqlType(SqlTypeName.MAP);
    case "Date":
      return typeFactory.createSqlType(SqlTypeName.DATE);
    case "DateTime":
    case "Timestamp":
      return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    case "BigDecimal":
    case "Decimal":
      return typeFactory.createSqlType(SqlTypeName.DECIMAL);
    case "Long":
      return typeFactory.createSqlType(SqlTypeName.BIGINT);
    default:
      return typeFactory.createSqlType(SqlTypeName.ANY);
    }
  }

  @Override
  public String toString() {
    return "GraphQLTableImpl:" + (getName() != null ? getName() : "unknown");
  }
}
