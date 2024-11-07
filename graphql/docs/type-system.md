# Type System

## Table of Contents
- [Overview](#overview)
- [Type Mappings](#type-mappings)
- [Type Conversion](#type-conversion)
- [Custom Types](#custom-types)
- [Type Handling](#type-handling)
- [Null Handling](#null-handling)

## Overview
The type system provides a robust mapping between SQL types and GraphQL types, ensuring type safety and accurate data representation across the system.

## Type Mappings

### Basic Types
| SQL Type | GraphQL Type | Java Type | Description |
|----------|--------------|-----------|-------------|
| INTEGER | Int | Integer | 32-bit integer |
| BIGINT | Int | Long | 64-bit integer |
| DECIMAL | BigDecimal | java.math.BigDecimal | Arbitrary precision decimal |
| FLOAT | Float | Float | 32-bit floating point |
| DOUBLE | Float | Double | 64-bit floating point |
| VARCHAR | String | String | Variable-length string |
| CHAR | String | String | Fixed-length string |
| BOOLEAN | Boolean | Boolean | Boolean value |
| DATE | Date | java.sql.Date | Date without time |
| TIME | String | java.sql.Time | Time without date |
| TIMESTAMP | DateTime | java.sql.Timestamp | Date and time |
| BINARY | String | byte[] | Binary data |
| ARRAY | List | List<?> | Array of values |
| NULL | null | null | Null value |

### Complex Types
| SQL Type | GraphQL Type | Java Type | Description |
|----------|--------------|-----------|-------------|
| STRUCT | Object | Map<String, Object> | Structured data |
| MAP | Object | Map<String, Object> | Key-value pairs |
| JSON | JSONObject | String | JSON data |
| XML | String | String | XML data |
| INTERVAL | String | org.apache.calcite.avatica.util.TimeUnitRange | Time intervals |

## Type Conversion

### Conversion Rules
```java
public class TypeConverter {
    public static Object convert(Object value, int sqlType) {
        if (value == null) return null;
        
        switch (sqlType) {
            case Types.INTEGER:
                return convertToInteger(value);
            case Types.BIGINT:
                return convertToLong(value);
            case Types.DECIMAL:
                return convertToDecimal(value);
            case Types.TIMESTAMP:
                return convertToTimestamp(value);
            // ... other conversions
        }
    }
    
    private static Object convertToTimestamp(Object value) {
        if (value instanceof String) {
            return Timestamp.valueOf((String) value);
        } else if (value instanceof Long) {
            return new Timestamp((Long) value);
        }
        throw new IllegalArgumentException("Cannot convert to timestamp: " + value);
    }
    
    // ... other conversion methods
}
```

### Type Inference
```java
public class TypeInference {
    public static int inferSqlType(GraphQLType type) {
        if (type instanceof GraphQLScalarType) {
            return inferScalarType((GraphQLScalarType) type);
        } else if (type instanceof GraphQLList) {
            return Types.ARRAY;
        } else if (type instanceof GraphQLObjectType) {
            return Types.STRUCT;
        }
        return Types.VARCHAR; // default
    }
    
    private static int inferScalarType(GraphQLScalarType type) {
        switch (type.getName()) {
            case "Int": return Types.INTEGER;
            case "Float": return Types.FLOAT;
            case "String": return Types.VARCHAR;
            case "Boolean": return Types.BOOLEAN;
            case "ID": return Types.VARCHAR;
            default: return Types.VARCHAR;
        }
    }
}
```

## Custom Types

### Custom Type Definition
```java
public class CustomType implements SqlUserDefinedType {
    private final String name;
    private final int sqlType;
    private final Class<?> javaClass;
    
    public CustomType(String name, int sqlType, Class<?> javaClass) {
        this.name = name;
        this.sqlType = sqlType;
        this.javaClass = javaClass;
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public int getSqlType() {
        return sqlType;
    }
    
    @Override
    public Class<?> getJavaClass() {
        return javaClass;
    }
}
```

### Custom Type Registration
```java
public class TypeRegistry {
    private final Map<String, CustomType> types = new HashMap<>();
    
    public void register(CustomType type) {
        types.put(type.getName(), type);
    }
    
    public CustomType get(String name) {
        return types.get(name);
    }
}
```

## Type Handling

### Null Handling
```java
public class NullHandler {
    public static boolean isNullable(RelDataType type) {
        return type.isNullable();
    }
    
    public static Object handleNull(Object value, RelDataType type) {
        if (value == null && !type.isNullable()) {
            throw new NullPointerException("Null value for non-nullable type: " + type);
        }
        return value;
    }
}
```

### Array Handling
```java
public class ArrayHandler {
    public static Object[] convertArray(List<?> list, int componentType) {
        if (list == null) return null;
        Object[] result = new Object[list.size()];
        for (int i = 0; i < list.size(); i++) {
            result[i] = TypeConverter.convert(list.get(i), componentType);
        }
        return result;
    }
}
```

## Null Handling

### Null Semantics
1. **Three-valued Logic**
   - TRUE
   - FALSE
   - UNKNOWN (NULL)

2. **Comparison Rules**
   - NULL = NULL → UNKNOWN
   - NULL <> NULL → UNKNOWN
   - NULL AND TRUE → UNKNOWN
   - NULL OR FALSE → UNKNOWN

### Null Propagation
```java
public class NullPropagation {
    public static Boolean evaluateAnd(Boolean left, Boolean right) {
        if (left == null || right == null) {
            return left == Boolean.FALSE || right == Boolean.FALSE 
                ? Boolean.FALSE : null;
        }
        return left && right;
    }
    
    public static Boolean evaluateOr(Boolean left, Boolean right) {
        if (left == null || right == null) {
            return left == Boolean.TRUE || right == Boolean.TRUE 
                ? Boolean.TRUE : null;
        }
        return left || right;
    }
}
```
