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
dependencies {
    api(project(":core"))
    api(project(":linq4j"))
    api("com.github.xiedeyantu:quidem")
    api("org.apache.calcite.avatica:avatica-core")
    api("org.checkerframework:checker-qual")

    implementation("com.google.guava:guava")
    implementation("com.teradata.tpcds:tpcds")
    implementation("cn.hutool:hutool-all")
    implementation("io.prestosql.tpch:tpch")
    implementation("net.hydromatic:chinook-data-hsqldb")
    implementation("net.hydromatic:tpcds")
    implementation("net.java.dev.jna:jna")
    implementation("net.java.dev.jna:jna-platform")

    implementation("com.github.oshi:oshi-core") {
        exclude("log4j", "log4j")
            .because("log4j is already present in the classpath")
        exclude("org.slf4j", "slf4j-api")
            .because("slf4j is already present in the classpath")
    }

    implementation("org.apache.calcite.avatica:avatica-server")
    implementation("org.hsqldb:hsqldb::jdk8")

    testImplementation("net.hydromatic:sql-logic-test")
    testImplementation(project(":testkit"))
    testImplementation("org.incava:java-diff")
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl")
}
