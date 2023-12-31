package org.apache.calcite.adapter.gremlin.converter.schema;

import org.apache.calcite.util.Pair;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.calcite.adapter.gremlin.converter.schema.calcite.GremlinSchema;
import org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinEdgeTable;
import org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinProperty;
import org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinVertexTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

import lombok.AllArgsConstructor;
import lombok.NonNull;

public final class SqlSchemaGrabber {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlSchemaGrabber.class);
    private static final Map<Class<?>, String> TYPE_MAP = new HashMap<>();
    private static final String VERTEX_EDGES_LABEL_QUERY = "g.V().hasLabel('%s').%sE().label().dedup()";
    private static final String PROPERTIES_VALUE_QUERY = "g.%s().hasLabel('%s').values('%s').%s";
    private static final String PROPERTY_KEY_QUERY = "g.%s().hasLabel('%s').properties().key().dedup()";
    private static final String LABELS_QUERY = "g.%s().label().dedup()";
    private static final String IN_OUT_VERTEX_QUERY =
            "g.E().hasLabel('%s').project('in','out').by(inV().label()).by(outV().label()).dedup()";

    static {
        TYPE_MAP.put(String.class, "String");
        TYPE_MAP.put(Boolean.class, "Boolean");
        TYPE_MAP.put(Byte.class, "Byte");
        TYPE_MAP.put(Short.class, "Short");
        TYPE_MAP.put(Integer.class, "Integer");
        TYPE_MAP.put(Long.class, "Long");
        TYPE_MAP.put(Float.class, "Float");
        TYPE_MAP.put(Double.class, "Double");
        TYPE_MAP.put(Date.class, "Date");
    }

    private SqlSchemaGrabber() {
    }

    public static GremlinSchema getSchema(final GraphTraversalSource g, final ScanType scanType) throws SQLException {
        final ExecutorService executor = Executors.newFixedThreadPool(96,
                new ThreadFactoryBuilder().setNameFormat("RxSessionRunner-%d").setDaemon(true).build());
        try {
            final Future<List<GremlinVertexTable>> gremlinVertexTablesFuture =
                    executor.submit(new RunGremlinQueryVertices(g, executor, scanType));
            final Future<List<GremlinEdgeTable>> gremlinEdgeTablesFuture =
                    executor.submit(new RunGremlinQueryEdges(g, executor, scanType));
            final GremlinSchema gremlinSchema =
                    new GremlinSchema(gremlinVertexTablesFuture.get(), gremlinEdgeTablesFuture.get());
            executor.shutdown();
            return gremlinSchema;
        } catch (final ExecutionException | InterruptedException e) {
            e.printStackTrace();
            executor.shutdown();
            throw new SQLException("Error occurred during schema collection. '" + e.getMessage() + "'.");
        }
    }

    private static String getType(final Set<?> data) {
        final Set<String> types = new HashSet<>();
        for (final Object d : data) {
            types.add(TYPE_MAP.getOrDefault(d.getClass(), "String"));
        }
        if (types.size() == 1) {
            return types.iterator().next();
        } else if (types.size() > 1) {
            if (types.contains("String") || types.contains("Date")) {
                return "String";
            } else if (types.contains("Double")) {
                return "Double";
            } else if (types.contains("Float")) {
                return "Float";
            } else if (types.contains("Long")) {
                return "Long";
            } else if (types.contains("Integer")) {
                return "Integer";
            } else if (types.contains("Short")) {
                return "Short";
            } else if (types.contains("Byte")) {
                return "Byte";
            }
        }
        return "String";
    }

    public enum ScanType {
        First("First"),
        All("All");

        private final String stringValue;

        ScanType(@NonNull final String stringValue) {
            this.stringValue = stringValue;
        }

        /**
         * Converts case-insensitive string to enum value.
         *
         * @param in The case-insensitive string to be converted to enum.
         * @return The enum value if string is recognized as a valid value, otherwise null.
         */
        public static ScanType fromString(@NonNull final String in) {
            for (final ScanType scheme : ScanType.values()) {
                if (scheme.stringValue.equalsIgnoreCase(in)) {
                    return scheme;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return this.stringValue;
        }
    }

    @AllArgsConstructor
    static
    class RunGremlinQueryVertices implements Callable<List<GremlinVertexTable>> {
        private final GraphTraversalSource g;
        private final ExecutorService service;
        private final ScanType scanType;

        @Override
        public List<GremlinVertexTable> call() throws Exception {
            final List<Future<List<GremlinProperty>>> gremlinProperties = new ArrayList<>();
            final List<Future<List<String>>> gremlinVertexInEdgeLabels = new ArrayList<>();
            final List<Future<List<String>>> gremlinVertexOutEdgeLabels = new ArrayList<>();
            final List<String> labels = service.submit(new RunGremlinQueryLabels(true, g)).get();

            for (final String label : labels) {
                gremlinProperties.add(service.submit(
                        new RunGremlinQueryPropertiesList(true, label, g, scanType, service)));
                gremlinVertexInEdgeLabels.add(service.submit(new RunGremlinQueryVertexEdges(g, label, "in")));
                gremlinVertexOutEdgeLabels.add(service.submit(new RunGremlinQueryVertexEdges(g, label, "out")));
            }

            final List<GremlinVertexTable> gremlinVertexTables = new ArrayList<>();
            for (int i = 0; i < labels.size(); i++) {
                gremlinVertexTables.add(new GremlinVertexTable(labels.get(i), gremlinProperties.get(i).get(),
                        gremlinVertexInEdgeLabels.get(i).get(), gremlinVertexOutEdgeLabels.get(i).get()));
            }
            return gremlinVertexTables;
        }
    }

    @AllArgsConstructor
    static class RunGremlinQueryEdges implements Callable<List<GremlinEdgeTable>> {
        private final GraphTraversalSource g;
        private final ExecutorService service;
        private final ScanType scanType;

        @Override
        public List<GremlinEdgeTable> call() throws Exception {
            final List<Future<List<GremlinProperty>>> futureTableColumns = new ArrayList<>();
            final List<Future<List<Pair<String, String>>>> inOutLabels = new ArrayList<>();
            final List<String> labels = service.submit(new RunGremlinQueryLabels(false, g)).get();

            for (final String label : labels) {
                futureTableColumns.add(service.submit(
                        new RunGremlinQueryPropertiesList(false, label, g, scanType, service)));
                inOutLabels.add(service.submit(new RunGremlinQueryInOutV(g, label)));
            }

            final List<GremlinEdgeTable> gremlinEdgeTables = new ArrayList<>();
            for (int i = 0; i < labels.size(); i++) {
                gremlinEdgeTables.add(new GremlinEdgeTable(labels.get(i), futureTableColumns.get(i).get(),
                        inOutLabels.get(i).get()));
            }
            return gremlinEdgeTables;
        }
    }

    @AllArgsConstructor
    static class RunGremlinQueryVertexEdges implements Callable<List<String>> {
        private final GraphTraversalSource g;
        private final String label;
        private final String direction;

        @Override
        public List<String> call() throws Exception {
            final String query = String.format(VERTEX_EDGES_LABEL_QUERY, label, direction);
            LOGGER.debug(String.format("Start %s%n", query));
            final List<String> labels = "in".equals(direction) ? g.V().hasLabel(label).inE().label().dedup().toList() :
                    g.V().hasLabel(label).outE().label().dedup().toList();
            LOGGER.debug(String.format("End %s%n", query));
            return labels;
        }
    }

    @AllArgsConstructor
    static class RunGremlinQueryPropertyType implements Callable<String> {
        private final boolean isVertex;
        private final String label;
        private final String property;
        private final GraphTraversalSource g;
        private final ScanType strategy;

        @Override
        public String call() {
            final String query = String.format(PROPERTIES_VALUE_QUERY, isVertex ? "V" : "E", label, property,
                    strategy.equals(ScanType.First) ? "next(1)" : "toSet()");
            LOGGER.debug(String.format("Start %s%n", query));
            final GraphTraversal<?, ?> graphTraversal = isVertex ? g.V() : g.E();
            graphTraversal.hasLabel(label).values(property);
            final HashSet<?> data =
                    new HashSet<>(strategy.equals(ScanType.First) ? graphTraversal.next(1) : graphTraversal.toList());
            LOGGER.debug(String.format("End %s%n", query));
            return getType(data);
        }
    }

    @AllArgsConstructor
    static class RunGremlinQueryPropertiesList implements Callable<List<GremlinProperty>> {
        private final boolean isVertex;
        private final String label;
        private final GraphTraversalSource g;
        private final ScanType scanType;
        private final ExecutorService service;

        @Override
        public List<GremlinProperty> call() throws ExecutionException, InterruptedException {
            final String query = String.format(PROPERTY_KEY_QUERY, isVertex ? "V" : "E", label);
            LOGGER.debug(String.format("Start %s%n", query));
            final List<String> properties = isVertex ?
                    g.V().hasLabel(label).properties().key().dedup().toList() :
                    g.E().hasLabel(label).properties().key().dedup().toList();

            final List<Future<String>> propertyTypes = new ArrayList<>();
            for (final String property : properties) {
                propertyTypes.add(service
                        .submit(new RunGremlinQueryPropertyType(isVertex, label, property, g, scanType)));
            }

            final List<GremlinProperty> columns = new ArrayList<>();
            for (int i = 0; i < properties.size(); i++) {
                columns.add(new GremlinProperty(properties.get(i), propertyTypes.get(i).get().toLowerCase()));
            }

            LOGGER.debug(String.format("End %s%n", query));
            return columns;
        }
    }

    @AllArgsConstructor
    static class RunGremlinQueryLabels implements Callable<List<String>> {
        private final boolean isVertex;
        private final GraphTraversalSource g;

        @Override
        public List<String> call() {
            final String query = String.format(LABELS_QUERY, isVertex ? "V" : "E");
            LOGGER.debug(String.format("Start %s%n", query));
            final List<String> labels = isVertex ? g.V().label().dedup().toList() : g.E().label().dedup().toList();
            LOGGER.debug(String.format("End %s%n", query));
            return labels;
        }
    }

    @AllArgsConstructor
    static class RunGremlinQueryInOutV implements Callable<List<Pair<String, String>>> {
        private final GraphTraversalSource g;
        private final String label;

        @Override
        public List<Pair<String, String>> call() {
            final String query = String.format(IN_OUT_VERTEX_QUERY, label);
            LOGGER.debug(String.format("Start %s%n", query));
            final List<Map<String, Object>> result = g.E().hasLabel(label).
                    project("in", "out").
                    by(__.inV().label()).
                    by(__.outV().label()).
                    dedup().toList();
            final List<Pair<String, String>> labels = new ArrayList<>();
            result.stream().iterator().forEachRemaining(map -> map.forEach((key, value) -> {
                labels.add(new Pair<>(map.get("in").toString(), map.get("out").toString()));
            }));
            LOGGER.debug(String.format("End %s%n", query));
            return labels;
        }
    }
}
