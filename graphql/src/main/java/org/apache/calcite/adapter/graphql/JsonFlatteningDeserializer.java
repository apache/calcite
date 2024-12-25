package org.apache.calcite.adapter.graphql;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import graphql.schema.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JsonFlatteningDeserializer extends StdDeserializer<Object> {
    private final Map<String, GraphQLOutputType> fieldTypes;
    private static final Logger LOGGER = LogManager.getLogger(JsonFlatteningDeserializer.class);

    public JsonFlatteningDeserializer(Map<String, GraphQLOutputType> fieldTypes) {
        super(Object.class);
        this.fieldTypes = fieldTypes != null ? fieldTypes : Collections.emptyMap();
    }

    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        JsonNode node = p.getCodec().readTree(p);
        if (node.isArray()) {
            List<Object> result = new ArrayList<>();
            for (JsonNode elem : node) {
                result.add(flatten(elem, ""));
            }
            return result;
        } else {
            return flatten(node, "");
        }
    }

    private Map<String, Object> flatten(JsonNode node, String prefix) {
        if (node.isObject()) {
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(node.fields(), Spliterator.ORDERED), false)
                    .flatMap(entry -> {
                        String key = entry.getKey();
                        JsonNode value = entry.getValue();
                        String fullKey = prefix.isEmpty() ? key : prefix + "." + key;

                        if (value.isObject()) {
                            return flatten(value, fullKey).entrySet().stream();
                        } else if (value.isArray()) {
                            List<Object> arrayResult = new ArrayList<>();
                            for (JsonNode elem : value) {
                                if (elem.isObject()) {
                                    arrayResult.add(flatten(elem, ""));
                                } else {
                                    arrayResult.add(extractValue(elem, key));
                                }
                            }
                            return Stream.of(Map.entry(fullKey, arrayResult));
                        } else {
                            return Stream.of(Map.entry(fullKey, extractValue(value, key)));
                        }
                    })
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1, v2) -> v2,  // If duplicate keys, take the last one
                        LinkedHashMap::new  // Use LinkedHashMap to preserve order
                    ));
        } else {
            return new LinkedHashMap<>(Map.of("", extractValue(node, "")));
        }
    }

    private Object extractValue(JsonNode node, String fieldName) {
        if (node.isTextual()) {
            String value = node.asText();
            // Check if this field has a special type that needs conversion
            if (fieldTypes.containsKey(fieldName)) {
                GraphQLOutputType type = fieldTypes.get(fieldName);
                GraphQLType unwrappedType = GraphQLTypeUtil.unwrapAll(type);
                if (unwrappedType instanceof GraphQLScalarType) {
                    try {
                        String typeName = ((GraphQLScalarType) unwrappedType).getName();
                        switch (typeName) {
                            case "Date":
                                return Date.valueOf(value);
                            case "Timestamp":
                                return Timestamp.valueOf(value);
                            case "Timestamptz":
                                return Timestamp.from(OffsetDateTime.parse(value).toInstant());
                        }
                    } catch (IllegalArgumentException | DateTimeParseException e) {
                        LOGGER.warn("Failed to parse special type for field {}: {}", fieldName, value);
                    }
                }
            }
            return value;
        } else if (node.isNumber()) {
            return node.numberValue();
        } else if (node.isBoolean()) {
            return node.booleanValue();
        } else if (node.isNull()) {
            return null;
        } else {
            return node.toString();
        }
    }
}
