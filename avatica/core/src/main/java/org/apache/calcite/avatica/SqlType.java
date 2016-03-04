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
package org.apache.calcite.avatica;

import org.apache.calcite.avatica.util.ByteString;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Extends the information in {@link java.sql.Types}.
 *
 * <p>The information in the following conversions tables
 * (from the JDBC 4.1 specification) is held in members of this class.
 *
 * <p>Table B-1: JDBC Types Mapped to Java Types
 *
 * <pre>
 * JDBC Type     Java Type
 * ============= =========================
 * CHAR          String
 * VARCHAR       String
 * LONGVARCHAR   String
 * NUMERIC       java.math.BigDecimal
 * DECIMAL       java.math.BigDecimal
 * BIT           boolean
 * BOOLEAN       boolean
 * TINYINT       byte
 * SMALLINT      short
 * INTEGER       int
 * BIGINT        long
 * REAL          float
 * FLOAT         double
 * DOUBLE        double
 * BINARY        byte[]
 * VARBINARY     byte[]
 * LONGVARBINARY byte[]
 * DATE          java.sql.Date
 * TIME          java.sql.Time
 * TIMESTAMP     java.sql.Timestamp
 * CLOB          java.sql.Clob
 * BLOB          java.sql.Blob
 * ARRAY         java.sql.Array
 * DISTINCT      mapping of underlying type
 * STRUCT        java.sql.Struct
 * REF           java.sql.Ref
 * DATALINK      java.net.URL
 * JAVA_OBJECT   underlying Java class
 * ROWID         java.sql.RowId
 * NCHAR         String
 * NVARCHAR      String
 * LONGNVARCHAR  String
 * NCLOB         java.sql.NClob
 * SQLXML        java.sql.SQLXML
 * </pre>
 *
 * <p>Table B-2: Standard Mapping from Java Types to JDBC Types
 *
 * <pre>
 * Java Type            JDBC Type
 * ==================== ==============================================
 * String               CHAR, VARCHAR, LONGVARCHAR, NCHAR, NVARCHAR or
 *                      LONGNVARCHAR
 * java.math.BigDecimal NUMERIC
 * boolean              BIT or BOOLEAN
 * byte                 TINYINT
 * short                SMALLINT
 * int                  INTEGER
 * long                 BIGINT
 * float                REAL
 * double               DOUBLE
 * byte[]               BINARY, VARBINARY, or LONGVARBINARY
 * java.sql.Date        DATE
 * java.sql.Time        TIME
 * java.sql.Timestamp   TIMESTAMP
 * java.sql.Clob        CLOB
 * java.sql.Blob        BLOB
 * java.sql.Array       ARRAY
 * java.sql.Struct      STRUCT
 * java.sql.Ref         REF
 * java.net.URL         DATALINK
 * Java class           JAVA_OBJECT
 * java.sql.RowId       ROWID
 * java.sql.NClob       NCLOB
 * java.sql.SQLXML      SQLXML
 * </pre>
 *
 * <p>TABLE B-3: Mapping from JDBC Types to Java Object Types
 *
 * <pre>
 * JDBC Type     Java Object Type
 * ============= ======================
 * CHAR          String
 * VARCHAR       String
 * LONGVARCHAR   String
 * NUMERIC       java.math.BigDecimal
 * DECIMAL       java.math.BigDecimal
 * BIT           Boolean
 * BOOLEAN       Boolean
 * TINYINT       Integer
 * SMALLINT      Integer
 * INTEGER       Integer
 * BIGINT        Long
 * REAL          Float
 * FLOAT         Double
 * DOUBLE        Double
 * BINARY        byte[]
 * VARBINARY     byte[]
 * LONGVARBINARY byte[]
 * DATE          java.sql.Date
 * TIME          java.sql.Time
 * TIMESTAMP     java.sql.Timestamp
 * DISTINCT      Object type of underlying type
 * CLOB          java.sql.Clob
 * BLOB          java.sql.Blob
 * ARRAY         java.sql.Array
 * STRUCT        java.sql.Struct or java.sql.SQLData
 * REF           java.sql.Ref
 * DATALINK      java.net.URL
 * JAVA_OBJECT   underlying Java class
 * ROWID         java.sql.RowId
 * NCHAR         String
 * NVARCHAR      String
 * LONGNVARCHAR  String
 * NCLOB         java.sql.NClob
 * SQLXML        java.sql.SQLXML
 * </pre>
 *
 * <p>TABLE B-4: Mapping from Java Object Types to JDBC Types
 *
 * <pre>
 * Java Object Type     JDBC Type
 * ==================== ===========================================
 * String               CHAR, VARCHAR, LONGVARCHAR, NCHAR, NVARCHAR
 *                      or LONGNVARCHAR
 * java.math.BigDecimal NUMERIC
 * Boolean              BIT or BOOLEAN
 * Byte                 TINYINT
 * Short                SMALLINT
 * Integer              INTEGER
 * Long                 BIGINT
 * Float                REAL
 * Double               DOUBLE
 * byte[]               BINARY, VARBINARY, or LONGVARBINARY
 * java.math.BigInteger BIGINT
 * java.sql.Date        DATE
 * java.sql.Time        TIME
 * java.sql.Timestamp   TIMESTAMP
 * java.sql.Clob        CLOB
 * java.sql.Blob        BLOB
 * java.sql.Array       ARRAY
 * java.sql.Struct      STRUCT
 * java.sql.Ref         REF
 * java.net.URL         DATALINK
 * Java class           JAVA_OBJECT
 * java.sql.RowId       ROWID
 * java.sql.NClob       NCLOB
 * java.sql.SQLXML      SQLXML
 * java.util.Calendar   TIMESTAMP
 * java.util.Date       TIMESTAMP
 * </pre>
 *
 * <p><a name="B5">TABLE B-5</a>: Conversions performed by {@code setObject} and
 * {@code setNull} between Java object types and target JDBC types
 *
 * <!--
 * CHECKSTYLE: OFF
 * -->
 * <pre>
 *                      T S I B R F D D N B B C V L B V L D T T A B C S R D J R N N L N S
 *                      I M N I E L O E U I O H A O I A O A I I R L L T E A A O C V O C Q
 *                      N A T G A O U C M T O A R N N R N T M M R O O R F T V W H A N L L
 *                      Y L E I L A B I E   L R C G A B G E E E A B B U   A A I A R G O X
 *                      I L G N   T L M R   E   H V R I V E   S Y     C   L _ D R C N B M
 *                      N I E T     E A I   A   A A Y N A     T       T   I O     H V   L
 *                      T N R         L C   N   R R   A R     A           N B     A A
 *                        T                       C   R B     M           K J     R R
 *                                                H   Y I     P                     C
 * Java type                                  
 * ==================== = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
 * String               x x x x x x x x x x x x x x x x x x x x . . . . . . . . x x x . .
 * java.math.BigDecimal x x x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * Boolean              x x x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * Byte                 x x x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * Short                x x x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * Integer              x x x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * Long                 x x x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * Float                x x x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * Double               x x x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * byte[]               . . . . . . . . . . . . . . x x x . . . . . . . . . . . . . . . .
 * java.math.BigInteger . . . x . . . . . . . x x x . . . . . . . . . . . . . . . . . . .
 * java.sql.Date        . . . . . . . . . . . x x x . . . x . x . . . . . . . . . . . . .
 * java.sql.Time        . . . . . . . . . . . x x x . . . . x x . . . . . . . . . . . . .
 * java.sql.Timestamp   . . . . . . . . . . . x x x . . . x x x . . . . . . . . . . . . .
 * java.sql.Array       . . . . . . . . . . . . . . . . . . . . x . . . . . . . . . . . .
 * java.sql.Blob        . . . . . . . . . . . . . . . . . . . . . x . . . . . . . . . . .
 * java.sql.Clob        . . . . . . . . . . . . . . . . . . . . . . x . . . . . . . . . .
 * java.sql.Struct      . . . . . . . . . . . . . . . . . . . . . . . x . . . . . . . . .
 * java.sql.Ref         . . . . . . . . . . . . . . . . . . . . . . . . x . . . . . . . .
 * java.net.URL         . . . . . . . . . . . . . . . . . . . . . . . . . x . . . . . . .
 * Java class           . . . . . . . . . . . . . . . . . . . . . . . . . . x . . . . . .
 * java.sql.Rowid       . . . . . . . . . . . . . . . . . . . . . . . . . . . x . . . . .
 * java.sql.NClob       . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . x .
 * java.sql.SQLXML      . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . x
 * java.util.Calendar   . . . . . . . . . . . x x x . . . x x x . . . . . . . . . . . . .
 * java.util.Date       . . . . . . . . . . . x x x . . . x x x . . . . . . . . . . . . .
 * </pre>
 * <!--
 * CHECKSTYLE: ON
 * -->
 *
 * <p><a name="B6">TABLE B-6</a>: Use of {@code ResultSet} getter methods to
 * retrieve JDBC data types
 *
 * <!--
 * CHECKSTYLE: OFF
 * -->
 * <pre>
 *                      T S I B R F D D N B B C V L B V L D T T C B A R D S J R N N L N S
 *                      I M N I E L O E U I O H A O I A O A I I L L R E A T A O C V O C Q
 *                      N A T G A O U C M T O A R N N R N T M M O O R F T R V W H A N L L
 *                      Y L E I L A B I E   L R C G A B G E E E B B A   A U A I A R G O X
 *                      I L G N   T L M R   E   H V R I V E   S     Y   L C _ D R C N B M
 *                      N I E T     E A I   A   A A Y N A     T         I T O     H V   L
 *                      T N R         L C   N   R R   A R     A         N   B     A A
 *                        T                       C   R B     M         K   J     R R
 *                                                H   Y I     P                     C
 * Java type                                                                
 * ==================== = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
 * getByte              X x x x x x x x x x x x x . . . . . . . . . . . . . . x . . . . .
 * getShort             x X x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * getInt               x x X x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * getLong              x x x X x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * getFloat             x x x x X x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * getDouble            x x x x x X X x x x x x x . . . . . . . . . . . . . . . . . . . .
 * getBigDecimal        x x x x x x x X X x x x x . . . . . . . . . . . . . . . . . . . .
 * getBoolean           x x x x x x x x x X x x x . . . . . . . . . . . . . . . . . . . .
 * getString            x x x x x x x x x x x X X x x x x x x x . . . . x . . . x x x . .
 * getNString           x x x x x x x x x x x x x x x x x x x x . . . . x . . . X X x . .
 * getBytes             . . . . . . . . . . . . . . X X x . . . . . . . . . . . . . . . .
 * getDate              . . . . . . . . . . . x x x . . . X . x . . . . . . . . . . . . .
 * getTime              . . . . . . . . . . . x x x . . . . X x . . . . . . . . . . . . .
 * getTimestamp         . . . . . . . . . . . x x x . . . x x X . . . . . . . . . . . x .
 * getAsciiStream       . . . . . . . . . . . x x X x x x . . . x . . . . . . . . . . . x
 * getBinaryStream      . . . . . . . . . . . . . . x x X . . . . x . . . . . . . . . x x
 * getCharacterStream   . . . . . . . . . . . x x X x x x . . . x . . . . . . . x x x x x
 * getNCharacterStream  . . . . . . . . . . . x x x x x x . . . x . . . . . . . x x X x x
 * getClob              . . . . . . . . . . . . . . . . . . . . X . . . . . . . . . . x .
 * getNClob             . . . . . . . . . . . . . . . . . . . . x . . . . . . . . . . X .
 * getBlob              . . . . . . . . . . . . . . . . . . . . . X . . . . . . . . . . .
 * getArray             . . . . . . . . . . . . . . . . . . . . . . X . . . . . . . . . .
 * getRef               . . . . . . . . . . . . . . . . . . . . . . . X . . . . . . . . .
 * getURL               . . . . . . . . . . . . . . . . . . . . . . . . X . . . . . . . .
 * getObject            x x x x x x x x x x x x x x x x x x x x x x x x x X X x x x x x x
 * getRowId             . . . . . . . . . . . . . . . . . . . . . . . . . . . X . . . . .
 * getSQLXML            . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . X
 * </pre>
 * <!--
 * CHECKSTYLE: ON
 * -->
 */
public enum SqlType {
  BIT(Types.BIT, boolean.class),
  BOOLEAN(Types.BOOLEAN, boolean.class),
  TINYINT(Types.TINYINT, byte.class),
  SMALLINT(Types.SMALLINT, short.class),
  INTEGER(Types.INTEGER, int.class),
  BIGINT(Types.BIGINT, long.class),
  NUMERIC(Types.NUMERIC, BigDecimal.class),
  DECIMAL(Types.DECIMAL, BigDecimal.class),
  FLOAT(Types.FLOAT, double.class),
  REAL(Types.REAL, float.class),
  DOUBLE(Types.DOUBLE, double.class),
  DATE(Types.DATE, java.sql.Date.class, int.class),
  TIME(Types.TIME, Time.class, int.class),
  TIMESTAMP(Types.TIMESTAMP, Timestamp.class, long.class),
  INTERVAL_YEAR_MONTH(Types.OTHER, Boolean.class),
  INTERVAL_DAY_TIME(Types.OTHER, Boolean.class),
  CHAR(Types.CHAR, String.class),
  VARCHAR(Types.VARCHAR, String.class),
  LONGVARCHAR(Types.LONGVARCHAR, String.class),
  BINARY(Types.BINARY, byte[].class, ByteString.class, String.class),
  VARBINARY(Types.VARBINARY, byte[].class, ByteString.class, String.class),
  LONGVARBINARY(Types.LONGVARBINARY, byte[].class, ByteString.class,
      String.class),
  NULL(Types.NULL, Void.class),
  ANY(Types.JAVA_OBJECT, Object.class),
  SYMBOL(Types.OTHER, Object.class),
  MULTISET(Types.ARRAY, List.class),
  ARRAY(Types.ARRAY, Array.class),
  BLOB(Types.BLOB, Blob.class),
  CLOB(Types.CLOB, Clob.class),
  SQLXML(Types.SQLXML, java.sql.SQLXML.class),
  MAP(Types.OTHER, Map.class),
  DISTINCT(Types.DISTINCT, Object.class),
  STRUCT(Types.STRUCT, Struct.class),
  REF(Types.REF, Ref.class),
  DATALINK(Types.DATALINK, URL.class),
  JAVA_OBJECT(Types.JAVA_OBJECT, Object.class),
  ROWID(Types.ROWID, RowId.class),
  NCHAR(Types.NCHAR, String.class),
  NVARCHAR(Types.NVARCHAR, String.class),
  LONGNVARCHAR(Types.LONGNVARCHAR, String.class),
  NCLOB(Types.NCLOB, NClob.class),
  ROW(Types.STRUCT, Object.class),
  OTHER(Types.OTHER, Object.class),
  CURSOR(2012, Object.class),
  COLUMN_LIST(Types.OTHER + 2, Object.class);

  /** Type id as appears in {@link java.sql.Types},
   * e.g. {@link java.sql.Types#INTEGER}. */
  public final int id;

  /** Default Java type for this SQL type, as described in table B-1. */
  public final Class clazz;

  /** Class used internally in Calcite to represent instances of this type. */
  public final Class internal;

  /** Class used to serialize values of this type as JSON. */
  public final Class serial;

  private static final Map<Integer, SqlType> BY_ID = new HashMap<>();
  static {
    for (SqlType sqlType : values()) {
      BY_ID.put(sqlType.id, sqlType);
    }
  }

  SqlType(int id, Class clazz, Class internal, Class serial) {
    this.id = id;
    this.clazz = clazz;
    this.internal = internal;
    this.serial = serial;
  }

  SqlType(int id, Class clazz, Class internal) {
    this(id, clazz, internal, internal);
  }

  SqlType(int id, Class clazz) {
    this(id, clazz, clazz, clazz);
  }

  public static SqlType valueOf(int type) {
    final SqlType sqlType = BY_ID.get(type);
    if (sqlType == null) {
      throw new IllegalArgumentException("Unknown SQL type " + type);
    }
    return sqlType;
  }

  /** Returns the boxed type. */
  public Class boxedClass() {
    return AvaticaUtils.box(clazz);
  }

  /** Returns the entries in JDBC table B-5. */
  public static Iterable<Map.Entry<Class, SqlType>> getSetConversions() {
    final ArrayList<Map.Entry<Class, SqlType>> list = new ArrayList<>();
    for (Map.Entry<Class, EnumSet<SqlType>> entry : SET_LIST.entrySet()) {
      for (SqlType sqlType : entry.getValue()) {
        list.add(new AbstractMap.SimpleEntry<>(entry.getKey(), sqlType));
      }
    }
    return list;
  }

  public static final Map<Class, EnumSet<SqlType>> SET_LIST;
  public static final Map<Method, EnumSet<SqlType>> GET_LIST;

  static {
    SET_LIST = new HashMap<>();
    GET_LIST = new HashMap<>();

    EnumSet<SqlType> numericTypes = EnumSet.of(TINYINT, SMALLINT, INTEGER,
        BIGINT, REAL, FLOAT, DOUBLE, DECIMAL, NUMERIC, BIT, BOOLEAN);
    Class[] numericClasses = {
      BigDecimal.class, Boolean.class, Byte.class, Short.class, Integer.class,
      Long.class, Float.class, Double.class
    };
    EnumSet<SqlType> charTypes = EnumSet.of(CHAR, VARCHAR, LONGVARCHAR);
    EnumSet<SqlType> ncharTypes = EnumSet.of(NCHAR, NVARCHAR, LONGNVARCHAR);
    EnumSet<SqlType> binaryTypes = EnumSet.of(BINARY, VARBINARY, LONGVARBINARY);
    EnumSet<SqlType> dateTimeTypes = EnumSet.of(DATE, TIME, TIMESTAMP);
    final EnumSet<SqlType> numericCharTypes = concat(numericTypes, charTypes);
    SET_LIST.put(String.class,
        concat(numericCharTypes, binaryTypes, dateTimeTypes, ncharTypes));
    for (Class clazz : numericClasses) {
      SET_LIST.put(clazz, numericCharTypes);
    }
    SET_LIST.put(byte[].class, binaryTypes);
    SET_LIST.put(BigInteger.class,
        EnumSet.of(BIGINT, CHAR, VARCHAR, LONGVARCHAR));
    SET_LIST.put(java.sql.Date.class,
        concat(charTypes, EnumSet.of(DATE, TIMESTAMP)));
    SET_LIST.put(Time.class,
        concat(charTypes, EnumSet.of(TIME, TIMESTAMP)));
    SET_LIST.put(Timestamp.class,
        concat(charTypes, EnumSet.of(DATE, TIME, TIMESTAMP)));
    SET_LIST.put(Array.class, EnumSet.of(ARRAY));
    SET_LIST.put(Blob.class, EnumSet.of(BLOB));
    SET_LIST.put(Clob.class, EnumSet.of(CLOB));
    SET_LIST.put(Struct.class, EnumSet.of(STRUCT));
    SET_LIST.put(Ref.class, EnumSet.of(REF));
    SET_LIST.put(URL.class, EnumSet.of(DATALINK));
    SET_LIST.put(Class.class, EnumSet.of(JAVA_OBJECT));
    SET_LIST.put(RowId.class, EnumSet.of(ROWID));
    SET_LIST.put(NClob.class, EnumSet.of(NCLOB));
    SET_LIST.put(java.sql.SQLXML.class, EnumSet.of(SQLXML));
    SET_LIST.put(Calendar.class,
        concat(charTypes, EnumSet.of(DATE, TIME, TIMESTAMP)));
    SET_LIST.put(java.util.Date.class,
        concat(charTypes, EnumSet.of(DATE, TIME, TIMESTAMP)));

    EnumSet<Method> numericMethods =
        EnumSet.of(Method.GET_BYTE, Method.GET_SHORT, Method.GET_INT,
            Method.GET_LONG, Method.GET_FLOAT, Method.GET_DOUBLE,
            Method.GET_BIG_DECIMAL, Method.GET_BOOLEAN);
    for (Method method : numericMethods) {
      GET_LIST.put(method, numericCharTypes);
    }
    GET_LIST.put(Method.GET_BYTE, EnumSet.of(ROWID));
    for (Method method : EnumSet.of(Method.GET_STRING, Method.GET_N_STRING)) {
      GET_LIST.put(method,
          concat(numericCharTypes, binaryTypes, dateTimeTypes,
              EnumSet.of(DATALINK), ncharTypes));
    }
    GET_LIST.put(Method.GET_BYTES, binaryTypes);
    GET_LIST.put(Method.GET_DATE,
        concat(charTypes, EnumSet.of(DATE, TIMESTAMP)));
    GET_LIST.put(Method.GET_TIME,
        concat(charTypes, EnumSet.of(TIME, TIMESTAMP)));
    GET_LIST.put(Method.GET_TIMESTAMP,
        concat(charTypes, EnumSet.of(DATE, TIME, TIMESTAMP)));
    GET_LIST.put(Method.GET_ASCII_STREAM,
        concat(charTypes, binaryTypes, EnumSet.of(CLOB, NCLOB)));
    GET_LIST.put(Method.GET_BINARY_STREAM,
        concat(binaryTypes, EnumSet.of(BLOB, SQLXML)));
    GET_LIST.put(Method.GET_CHARACTER_STREAM,
        concat(charTypes, binaryTypes, ncharTypes,
            EnumSet.of(CLOB, NCLOB, SQLXML)));
    GET_LIST.put(Method.GET_N_CHARACTER_STREAM,
        concat(
            charTypes, binaryTypes, ncharTypes, EnumSet.of(CLOB, NCLOB, SQLXML)));
    GET_LIST.put(Method.GET_CLOB, EnumSet.of(CLOB, NCLOB));
    GET_LIST.put(Method.GET_N_CLOB, EnumSet.of(CLOB, NCLOB));
    GET_LIST.put(Method.GET_BLOB, EnumSet.of(BLOB));
    GET_LIST.put(Method.GET_ARRAY, EnumSet.of(ARRAY));
    GET_LIST.put(Method.GET_REF, EnumSet.of(REF));
    GET_LIST.put(Method.GET_BLOB, EnumSet.of(BLOB));
    GET_LIST.put(Method.GET_URL, EnumSet.of(DATALINK));
    GET_LIST.put(Method.GET_OBJECT, EnumSet.allOf(SqlType.class));
    GET_LIST.put(Method.GET_ROW_ID, EnumSet.of(ROWID));
    GET_LIST.put(Method.GET_SQLXML, EnumSet.of(SQLXML));
  }

  @SafeVarargs
  private static <E extends Enum<E>> EnumSet<E> concat(Collection<E>... ess) {
    final List<E> list = new ArrayList<>();
    for (Collection<E> es : ess) {
      list.addAll(es);
    }
    return EnumSet.copyOf(list);
  }

  /** Returns whether {@link java.sql.PreparedStatement#setObject} and
   * {@link PreparedStatement#setNull} can assign a value of a particular class
   * to a column of a particular SQL type.
   *
   * <p>The JDBC standard describes the mapping in table <a href="#B5">B-5</a>.
   */
  public static boolean canSet(Class aClass, SqlType sqlType) {
    final EnumSet<SqlType> sqlTypes = SET_LIST.get(aClass);
    return sqlTypes != null && sqlTypes.contains(sqlType);
  }

  /** Returns whether {@link java.sql.ResultSet#getInt(int)} and similar methods
   * can convert a value to a particular SQL type.
   *
   * <p>The JDBC standard describes the mapping in table <a href="#B6">B-6</a>.
   */
  public static boolean canGet(Method method, SqlType sqlType) {
    final EnumSet<SqlType> sqlTypes = GET_LIST.get(method);
    return sqlTypes != null && sqlTypes.contains(sqlType);
  }

  /** Getter methods in {@link java.sql.ResultSet}. */
  public enum Method {
    GET_BYTE("getByte"),
    GET_SHORT("getShort"),
    GET_INT("getInt"),
    GET_LONG("getLong"),
    GET_FLOAT("getFloat"),
    GET_DOUBLE("getDouble"),
    GET_BIG_DECIMAL("getBigDecimal"),
    GET_BOOLEAN("getBoolean"),
    GET_STRING("getString"),
    GET_N_STRING("getNString"),
    GET_BYTES("getBytes"),
    GET_DATE("getDate"),
    GET_TIME("getTime"),
    GET_TIMESTAMP("getTimestamp"),
    GET_ASCII_STREAM("getAsciiStream"),
    GET_BINARY_STREAM("getBinaryStream"),
    GET_CHARACTER_STREAM("getCharacterStream"),
    GET_N_CHARACTER_STREAM("getNCharacterStream"),
    GET_CLOB("getClob"),
    GET_N_CLOB("getNClob"),
    GET_BLOB("getBlob"),
    GET_ARRAY("getArray"),
    GET_REF("getRef"),
    GET_URL("getURL"),
    GET_OBJECT("getObject"),
    GET_ROW_ID("getRowId"),
    GET_SQLXML("getSQLXML");

    public final String methodName;

    Method(String methodName) {
      this.methodName = methodName;
    }
  }
}

// End SqlType.java
