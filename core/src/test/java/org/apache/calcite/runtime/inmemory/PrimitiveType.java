package org.apache.calcite.runtime.inmemory;

import java.util.ArrayList;
import java.util.List;

public final class PrimitiveType {
    public int INT_;
    public char CHAR_;

    public PrimitiveType(final int int_, final char char_) {
        INT_ = int_;
        CHAR_ = char_;
    }

    @Override
    public final String toString() {
        return INT_+";"+CHAR_;
    }

    @Override
    public final boolean equals(final Object other) {
        return null != other && hashCode() == other.hashCode();
    }

    @Override
    public final int hashCode() {
        List<Object> set = new ArrayList<>();
        set.add(INT_);
        set.add(CHAR_);
        return set.hashCode();
    }
}
