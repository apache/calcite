package org.apache.calcite.adapter.graphql;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.linq4j.tree.Types;
import java.lang.reflect.Method;

/**
 * Built-in methods in the GraphQL adapter.
 *
 * @see org.apache.calcite.util.BuiltInMethod
 */
@SuppressWarnings("ImmutableEnumChecker")
enum GraphQLMethod {
  GRAPHQL_QUERY(GraphQLTable.class, "query", String.class);

  final Method method;

  static final ImmutableMap<Method, GraphQLMethod> MAP;

  static {
    final ImmutableMap.Builder<Method, GraphQLMethod> builder =
        ImmutableMap.builder();
    for (GraphQLMethod value : GraphQLMethod.values()) {
      builder.put(value.method, value);
    }
    MAP = builder.build();
  }

  /** Defines a method. */
  GraphQLMethod(Class<?> clazz, String methodName, Class<?>... argumentTypes) {
    this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
  }
}
