package net.hydromatic.optiq;

import org.eigenbase.reltype.RelDataType;

import java.util.List;

/**
 * A named expression in a schema.
 *
 * <h2>Examples of functions</h2>
 *
 * <p>Several kinds of functions crop up in real life. They all implement the
 * {@code Function} interface, but tend to be treated differently by the
 * back-end system if not by OPTIQ.</p>
 *
 * <p>A function that has zero arguments and a type that is a collection of
 * records is referred to as a <i>relation</i>. In schemas backed by a
 * relational database, tables and views will appear as relations.</p>
 *
 * <p>A function that has one or more arguments and a type that is a collection
 * of records is referred to as a <i>parameterized relation</i>. Some relational
 * databases support these; for example, Oracle calls them "table
 * functions".</p>
 *
 * <p>Functions may be also more typical of programming-language functions:
 * they take zero or more arguments, and return a result of arbitrary type.</p>
 *
 * <p>From the above definitions, you can see that a relation is a special
 * kind of function. This makes sense, because even though it has no
 * arguments, it is "evaluated" each time it is used in a query.</p>
 */
public interface Function extends SchemaObject {
    /**
     * Returns the parameters of this function.
     *
     * @return Parameters; never null
     */
    List<Parameter> getParameters();

    /**
     * Returns the type of this function's result.
     *
     * @return Type of result; never null
     */
    RelDataType getType();
}

// End Function.java
