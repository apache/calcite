/**
 * Object model for Java expressions.
 *
 * <p>This object model is used when the linq4j system is analyzing
 * queries that have been submitted using methods on the
 * {@link net.hydromatic.linq4j.Queryable} interface. The system attempts
 * to understand the intent of the query and reorganize it for
 * efficiency; for example, it may attempt to push down filters to the
 * source SQL system.</p>
 */
package net.hydromatic.linq4j.expressions;

// End package-info.java
