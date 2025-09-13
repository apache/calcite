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
plugins {
    `java-platform`
}

val String.v: String get() = rootProject.extra["$this.version"] as String

// Note: Gradle allows to declare dependency on "bom" as "api",
// and it makes the constraints to be transitively visible
// However Maven can't express that, so the approach is to use Gradle resolution
// and generate pom files with resolved versions
// See https://github.com/gradle/gradle/issues/9866

fun DependencyConstraintHandlerScope.apiv(
    notation: String,
    versionProp: String = notation.substringAfterLast(':')
) =
    "api"(notation + ":" + versionProp.v)

fun DependencyConstraintHandlerScope.runtimev(
    notation: String,
    versionProp: String = notation.substringAfterLast(':')
) =
    "runtime"(notation + ":" + versionProp.v)

javaPlatform {
    allowDependencies()
}

dependencies {
    api(platform("com.fasterxml.jackson:jackson-bom:${"jackson".v}"))

    // Parenthesis are needed here: https://github.com/gradle/gradle/issues/9248
    (constraints) {
        // api means "the dependency is for both compilation and runtime"
        // runtime means "the dependency is only for runtime, not for compilation"
        // In other words, marking dependency as "runtime" would avoid accidental
        // dependency on it during compilation
        apiv("cn.hutool:hutool-all")
        apiv("com.alibaba.database:innodb-java-reader")
        apiv("com.beust:jcommander")
        apiv("org.apache.cassandra:java-driver-core", "cassandra-java-driver-core")
        apiv("com.github.kstyrc:embedded-redis")
        apiv("com.github.oshi:oshi-core")
        apiv("com.github.stephenc.jcip:jcip-annotations")
        apiv("com.google.errorprone:error_prone_annotations", "errorprone")
        apiv("com.google.errorprone:error_prone_type_annotations", "errorprone")
        apiv("com.google.guava:guava")
        apiv("com.google.protobuf:protobuf-java", "protobuf")
        apiv("com.google.uzaygezen:uzaygezen-core", "uzaygezen")
        apiv("com.h2database:h2")
        apiv("com.jayway.jsonpath:json-path")
        apiv("com.joestelmach:natty")
        apiv("com.oracle.ojdbc:ojdbc8")
        apiv("com.teradata.tpcds:tpcds", "teradata.tpcds")
        apiv("com.yahoo.datasketches:sketches-core")
        apiv("commons-codec:commons-codec")
        apiv("commons-io:commons-io")
        apiv("org.checkerframework:checker-qual", "checkerframework")
        apiv("org.locationtech.jts:jts-core")
        apiv("org.locationtech.jts.io:jts-io-common")
        apiv("org.locationtech.proj4j:proj4j")
        apiv("org.locationtech.proj4j:proj4j-epsg", "proj4j")
        apiv("de.bwaldvogel:mongo-java-server", "mongo-java-server")
        apiv("de.bwaldvogel:mongo-java-server-core", "mongo-java-server")
        apiv("de.bwaldvogel:mongo-java-server-memory-backend", "mongo-java-server")
        apiv("io.prestosql.tpch:tpch")
        apiv("javax.servlet:javax.servlet-api", "servlet")
        apiv("joda-time:joda-time")
        apiv("junit:junit", "junit4")
        apiv("mysql:mysql-connector-java")
        apiv("net.hydromatic:aggdesigner-algorithm")
        apiv("net.hydromatic:chinook-data-hsqldb")
        apiv("net.hydromatic:foodmart-data-hsqldb")
        apiv("net.hydromatic:foodmart-data-json")
        apiv("net.hydromatic:foodmart-queries")
        apiv("net.hydromatic:quidem")
        apiv("net.hydromatic:scott-data-hsqldb")
        apiv("net.hydromatic:sql-logic-test")
        apiv("net.hydromatic:steelwheels-data-hsqldb")
        apiv("net.hydromatic:tpcds", "hydromatic.tpcds")
        apiv("net.minidev:json-smart")
        apiv("net.java.dev.jna:jna")
        apiv("net.java.dev.jna:jna-platform")
        apiv("net.sf.opencsv:opencsv")
        apiv("org.apache.arrow:arrow-memory-netty", "arrow")
        apiv("org.apache.arrow:arrow-vector", "arrow")
        apiv("org.apache.arrow:arrow-jdbc", "arrow")
        apiv("org.apache.arrow.gandiva:arrow-gandiva", "arrow-gandiva")
        apiv("org.apache.calcite.avatica:avatica-core", "calcite.avatica")
        apiv("org.apache.calcite.avatica:avatica-server", "calcite.avatica")
        apiv("org.apache.cassandra:cassandra-all")
        apiv("org.apache.commons:commons-dbcp2")
        apiv("org.apache.commons:commons-lang3")
        apiv("org.apache.commons:commons-math3")
        apiv("org.apache.commons:commons-pool2")
        apiv("org.apache.commons:commons-collections4")
        apiv("org.apache.commons:commons-text")
        apiv("org.apache.geode:geode-core")
        apiv("org.apache.hadoop:hadoop-client", "hadoop")
        apiv("org.apache.hadoop:hadoop-common", "hadoop")
        apiv("org.apache.httpcomponents:httpclient")
        apiv("org.apache.httpcomponents:httpcore")
        apiv("org.apache.kafka:kafka-clients")
        apiv("org.apache.kerby:kerb-client", "kerby")
        apiv("org.apache.kerby:kerb-core", "kerby")
        apiv("org.apache.kerby:kerb-simplekdc", "kerby")
        apiv("org.apache.logging.log4j:log4j-api", "log4j2")
        apiv("org.apache.logging.log4j:log4j-core", "log4j2")
        apiv("org.apache.logging.log4j:log4j-slf4j-impl", "log4j2")
        apiv("org.apache.pig:pig")
        apiv("org.apache.pig:pigunit", "pig")
        apiv("org.apache.spark:spark-core_2.10", "spark")
        apiv("org.apiguardian:apiguardian-api")
        apiv("net.bytebuddy:byte-buddy")
        apiv("org.cassandraunit:cassandra-unit")
        apiv("org.codehaus.janino:commons-compiler", "janino")
        apiv("org.codehaus.janino:janino")
        apiv("org.codelibs.elasticsearch.module:lang-painless", "elasticsearch")
        apiv("org.codelibs.elasticsearch.module:scripting-painless-spi", "elasticsearch")
        apiv("org.eclipse.jetty:jetty-http", "jetty")
        apiv("org.eclipse.jetty:jetty-security", "jetty")
        apiv("org.eclipse.jetty:jetty-server", "jetty")
        apiv("org.eclipse.jetty:jetty-util", "jetty")
        apiv("org.elasticsearch.client:elasticsearch-rest-client", "elasticsearch")
        apiv("org.elasticsearch.plugin:transport-netty4-client", "elasticsearch")
        apiv("org.elasticsearch:elasticsearch")
        apiv("org.immutables:value-annotations", "immutables")
        apiv("org.immutables:value", "immutables")
        apiv("org.exparity:hamcrest-date")
        apiv("org.hamcrest:hamcrest")
        apiv("org.hamcrest:hamcrest-core", "hamcrest")
        apiv("org.hamcrest:hamcrest-library", "hamcrest")
        apiv("org.hsqldb:hsqldb")
        apiv("org.incava:java-diff")
        apiv("org.jboss:jandex")
        apiv("org.jooq:joou-java-6", "joou")
        apiv("org.jsoup:jsoup")
        apiv("org.junit:junit-bom", "junit5")
        apiv("org.mockito:mockito-core", "mockito")
        apiv("org.mongodb:mongodb-driver-sync")
        apiv("org.ow2.asm:asm")
        apiv("org.ow2.asm:asm-all", "asm")
        apiv("org.ow2.asm:asm-analysis", "asm")
        apiv("org.ow2.asm:asm-commons", "asm")
        apiv("org.ow2.asm:asm-tree", "asm")
        apiv("org.ow2.asm:asm-util", "asm")
        apiv("org.postgresql:postgresql")
        apiv("org.scala-lang:scala-library")
        apiv("org.slf4j:slf4j-api", "slf4j")
        // TODO: https://issues.apache.org/jira/browse/CALCITE-4862
        // Eventually we should get rid of slf4j-log4j12 dependency but currently it is not possible
        // since certain modules (Pig, Piglet) have dependencies using directly Log4j 1.x APIs
        runtimev("org.slf4j:slf4j-log4j12", "slf4j")
        apiv("org.testcontainers:testcontainers")
        apiv("redis.clients:jedis")
        apiv("sqlline:sqlline")
        runtimev("org.openjdk.jmh:jmh-core", "jmh")
        apiv("org.openjdk.jmh:jmh-generator-annprocess", "jmh")
        runtimev("xalan:xalan")
        runtimev("xerces:xercesImpl")
        apiv("com.google.code.findbugs:jsr305")
    }
}
