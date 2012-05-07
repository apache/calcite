package net.hydromatic.optiq.jdbc;

/**
 * Replica of LINQ's Enumerator interface.
 *
 * <p>Package-protected; not part of JDBC.</p>
 */
interface OptiqEnumerator<T> {
    boolean moveNext();
    T current();
    void reset();
}

// End OptiqEnumerator.java
