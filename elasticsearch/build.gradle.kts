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
    id("com.github.vlsi.gradle-extensions")
}

dependencies {
    api(project(":core"))
    api(project(":linq4j"))

    api("com.fasterxml.jackson.core:jackson-annotations")
    api("com.fasterxml.jackson.core:jackson-core")
    api("com.fasterxml.jackson.core:jackson-databind")
    api("org.elasticsearch.client:elasticsearch-rest-client")
    api("org.slf4j:slf4j-api")

    implementation("com.google.guava:guava")
    implementation("org.apache.calcite.avatica:avatica-core")
    implementation("org.apache.httpcomponents:httpasyncclient")
    implementation("org.apache.httpcomponents:httpclient")
    implementation("org.apache.httpcomponents:httpcore")
    implementation("org.checkerframework:checker-qual")

    testImplementation("org.apache.logging.log4j:log4j-api")
    testImplementation("org.apache.logging.log4j:log4j-core")
    testImplementation("org.codelibs.elasticsearch.module:lang-painless")
    testImplementation("org.elasticsearch.plugin:transport-netty4-client")
    testImplementation("org.elasticsearch:elasticsearch")
    testImplementation(project(":testkit"))
    testRuntimeOnly("net.java.dev.jna:jna")
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl")
}
