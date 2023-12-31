package org.apache.calcite.adapter.gremlin;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class GraphConstants {
    public static final String STRING_VALUE = "value";
    public static final Long LONG_VALUE = 100L;
    public static final Integer INTEGER_VALUE = LONG_VALUE.intValue();
    public static final Short SHORT_VALUE = LONG_VALUE.shortValue();
    public static final Byte BYTE_VALUE = LONG_VALUE.byteValue();
    public static final Double DOUBLE_VALUE = LONG_VALUE.doubleValue();
    public static final Float FLOAT_VALUE = LONG_VALUE.floatValue();
    public static final Date DATE_VALUE;
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-mm-dd");

    static {
        Date date = null;
        try {
            date = DATE_FORMATTER.parse("1993-03-30");
        } catch (final ParseException e) {
            e.printStackTrace();
        }
        DATE_VALUE = date;
    }
}
