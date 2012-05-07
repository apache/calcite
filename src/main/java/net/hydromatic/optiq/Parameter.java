package net.hydromatic.optiq;

import org.eigenbase.reltype.RelDataType;

/**
 * Parameter to a {@link net.hydromatic.optiq.Function}.
 */
public interface Parameter {
    /**
     * Zero-based ordinal of this parameter within the function's parameter
     * list.
     *
     * @return Parameter ordinal
     */
    int getOrdinal();

    /**
     * Name of the parameter.
     *
     * @return Parameter name
     */
    String getName();

    /**
     * Returns the type of this parameter.
     *
     * @return Parameter type.
     */
    RelDataType getType();
}

// End Parameter.java
