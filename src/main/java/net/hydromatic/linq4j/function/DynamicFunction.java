package net.hydromatic.linq4j.function;

/**
 * Function that can be dynamically invoked.
 *
 * <p>If a function does not support this interface, you can call it using
 * {@link Functions#dynamicInvoke(Function, Object...)}.
 */
public interface DynamicFunction<R> extends Function<R> {
    R dynamicInvoke(Object... arguments);
}

// End DynamicFunction.java
