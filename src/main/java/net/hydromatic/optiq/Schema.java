package net.hydromatic.optiq;

import net.hydromatic.linq4j.expressions.Expression;

import java.util.List;
import java.util.Map;

/**
 * A namespace that returns relations and functions.
 *
 * <p>Most of the time, {@link #get(String)} return a {@link Function}.
 * The most common and important type of Function is the one with no
 * arguments and a result type that is a collection of records. It is
 * equivalent to a table in a relational database.</p>
 *
 * <p>For example, the query</p>
 *
 * <blockquote>select * from sales.emps</blockquote>
 *
 * <p>is valid if "sales" is a registered
 * schema and "emps" is a function with zero parameters and a result type
 * of {@code Collection(Record(int: "empno", String: "name"))}.</p>
 *
 * <p>If there are overloaded functions, then there may be more than one
 * object with a particular name. In this case, {@link #get} returns
 * an {@link Overload}.</p>
 *
 * <p>A schema is a {@link SchemaObject}, which implies that schemas can
 * be nested within schemas.</p>
 */
public interface Schema {
    SchemaObject get(String name);

    Map<String, SchemaObject> asMap();

    /**
     * Returns the expression for a sub-object "name" or "name(argument, ...)",
     * given an expression for the schema.
     *
     * <p>For example, given schema based on reflection, if the schema is
     * based on an object of type "class Foodmart" called "foodmart", then
     * the "emps" member would be accessed via "foodmart.emps". If the schema
     * were a map, the expression would be
     * "(Enumerable&lt;Employee&gt;) foodmart.get(&quot;emps&quot;)".</p>
     *
     * @param schemaExpression Expression for schema
     * @param name Name of schema object
     * @param arguments Arguments to schema object (null means no argument list)
     * @return Expression for a given schema object
     */
    Expression getExpression(
        Expression schemaExpression,
        SchemaObject schemaObject,
        String name,
        List<Expression> arguments);
}

// End Schema.java
