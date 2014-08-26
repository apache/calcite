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
package net.hydromatic.optiq.impl.java;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.*;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.*;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import com.google.common.collect.*;

import java.lang.reflect.*;
import java.util.*;

/**
 * Implementation of {@link net.hydromatic.optiq.Schema} that exposes the public
 * fields and methods in a Java object.
 */
public class ReflectiveSchema
    extends AbstractSchema {
  final Class clazz;
  private Object target;

  /**
   * Creates a ReflectiveSchema.
   *
   * @param target Object whose fields will be sub-objects of the schema
   */
  public ReflectiveSchema(Object target) {
    super();
    this.clazz = target.getClass();
    this.target = target;
  }

  @Override
  public String toString() {
    return "ReflectiveSchema(target=" + target + ")";
  }

  /** Returns the wrapped object. (May not appear to be used, but is used in
   * generated code via {@link BuiltinMethod#REFLECTIVE_SCHEMA_GET_TARGET}.) */
  public Object getTarget() {
    return target;
  }

  @Override
  protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (Field field : clazz.getFields()) {
      final String fieldName = field.getName();
      final Table table = fieldRelation(field);
      if (table == null) {
        continue;
      }
      builder.put(fieldName, table);
    }
    return builder.build();
  }

  @Override protected Multimap<String, Function> getFunctionMultimap() {
    final ImmutableMultimap.Builder<String, Function> builder =
        ImmutableMultimap.builder();
    for (Method method : clazz.getMethods()) {
      final String methodName = method.getName();
      if (method.getDeclaringClass() == Object.class
          || methodName.equals("toString")) {
        continue;
      }
      if (TranslatableTable.class.isAssignableFrom(method.getReturnType())) {
        final TableMacro tableMacro =
            new MethodTableMacro(this, method);
        builder.put(methodName, tableMacro);
      }
    }
    return builder.build();
  }

  /** Returns an expression for the object wrapped by this schema (not the
   * schema itself). */
  Expression getTargetExpression(SchemaPlus parentSchema, String name) {
    return Types.castIfNecessary(
        target.getClass(),
        Expressions.call(
            Schemas.unwrap(
                getExpression(parentSchema, name),
                ReflectiveSchema.class),
            BuiltinMethod.REFLECTIVE_SCHEMA_GET_TARGET.method));
  }

  /** Returns a table based on a particular field of this schema. If the
   * field is not of the right type to be a relation, returns null. */
  private <T> Table fieldRelation(final Field field) {
    final Type elementType = getElementType(field.getType());
    if (elementType == null) {
      return null;
    }
    Object o;
    try {
      o = field.get(target);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(
          "Error while accessing field " + field, e);
    }
    @SuppressWarnings("unchecked")
    final Enumerable<T> enumerable = toEnumerable(o);
    return new FieldTable<T>(field, elementType, enumerable);
  }

  /** Deduces the element type of a collection;
   * same logic as {@link #toEnumerable} */
  private static Type getElementType(Class clazz) {
    if (clazz.isArray()) {
      return clazz.getComponentType();
    }
    if (Iterable.class.isAssignableFrom(clazz)) {
      return Object.class;
    }
    return null; // not a collection/array/iterable
  }

  private static Enumerable toEnumerable(final Object o) {
    if (o.getClass().isArray()) {
      if (o instanceof Object[]) {
        return Linq4j.asEnumerable((Object[]) o);
      } else {
        return Linq4j.asEnumerable(Primitive.asList(o));
      }
    }
    if (o instanceof Iterable) {
      return Linq4j.asEnumerable((Iterable) o);
    }
    throw new RuntimeException(
        "Cannot convert " + o.getClass() + " into a Enumerable");
  }

  /** Table that is implemented by reading from a Java object. */
  private static class ReflectiveTable
      extends AbstractQueryableTable
      implements Table {
    private final Type elementType;
    private final Enumerable enumerable;

    public ReflectiveTable(Type elementType, Enumerable enumerable) {
      super(elementType);
      this.elementType = elementType;
      this.enumerable = enumerable;
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return ((JavaTypeFactory) typeFactory).createType(elementType);
    }

    public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }

    public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
        SchemaPlus schema, String tableName) {
      return new AbstractTableQueryable<T>(queryProvider, schema, this,
          tableName) {
        @SuppressWarnings("unchecked")
        public Enumerator<T> enumerator() {
          return (Enumerator<T>) enumerable.enumerator();
        }
      };
    }
  }

  /** Factory that creates a schema by instantiating an object and looking at
   * its public fields.
   *
   * <p>The following example instantiates a {@code FoodMart} object as a schema
   * that contains tables called {@code EMPS} and {@code DEPTS} based on the
   * object's fields.</p>
   *
   * <pre>
   * {@code schemas: [
   *     {
   *       name: "foodmart",
   *       type: "custom",
   *       factory: "net.hydromatic.optiq.impl.java.ReflectiveSchema$Factory",
   *       operand: {
   *         class: "com.acme.FoodMart",
   *         staticMethod: "instance"
   *       }
   *     }
   *   ]
   *
   * class FoodMart {
   *   public static final FoodMart instance() {
   *     return new FoodMart();
   *   }
   *
   *   Employee[] EMPS;
   *   Department[] DEPTS;
   * }
   * }</pre>
   * */
  public static class Factory implements SchemaFactory {
    public Schema create(SchemaPlus parentSchema, String name,
        Map<String, Object> operand) {
      Class clazz;
      Object target;
      final Object className = operand.get("class");
      if (className != null) {
        try {
          clazz = Class.forName((String) className);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Error loading class " + className, e);
        }
      } else {
        throw new RuntimeException("Operand 'class' is required");
      }
      final Object methodName = operand.get("staticMethod");
      if (methodName != null) {
        try {
          //noinspection unchecked
          Method method = clazz.getMethod((String) methodName);
          target = method.invoke(null);
        } catch (Exception e) {
          throw new RuntimeException("Error invoking method " + methodName, e);
        }
      } else {
        try {
          target = clazz.newInstance();
        } catch (Exception e) {
          throw new RuntimeException("Error instantiating class " + className,
              e);
        }
      }
      return new ReflectiveSchema(target);
    }
  }

  /** Table macro based on a Java method. */
  private static class MethodTableMacro extends ReflectiveFunctionBase
      implements TableMacro {
    private final ReflectiveSchema schema;

    public MethodTableMacro(ReflectiveSchema schema, Method method) {
      super(method);
      this.schema = schema;
      assert TranslatableTable.class.isAssignableFrom(method.getReturnType())
          : "Method should return TranslatableTable so the macro can be "
            + "expanded";
    }

    public String toString() {
      return "Member {method=" + method + "}";
    }

    public TranslatableTable apply(final List<Object> arguments) {
      try {
        final Object o = method.invoke(schema.getTarget(), arguments.toArray());
        return (TranslatableTable) o;
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Table based on a Java field. */
  private static class FieldTable<T> extends ReflectiveTable {
    private final Field field;

    public FieldTable(Field field, Type elementType, Enumerable<T> enumerable) {
      super(elementType, enumerable);
      this.field = field;
    }

    public String toString() {
      return "Relation {field=" + field.getName() + "}";
    }

    @Override
    public Expression getExpression(SchemaPlus schema, String tableName,
        Class clazz) {
      return Expressions.field(
          schema.unwrap(ReflectiveSchema.class).getTargetExpression(
              schema.getParentSchema(), schema.getName()), field);
    }
  }
}

// End ReflectiveSchema.java
