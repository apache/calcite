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
    kotlin("jvm")
    kotlin("kapt")
}

dependencies {
    api(project(":core"))
    api(project(":linq4j"))
    api("com.fasterxml.jackson.core:jackson-core")
    api("joda-time:joda-time")
    api("org.apache.calcite.avatica:avatica-core")
    api("org.checkerframework:checker-qual")
    api("org.slf4j:slf4j-api")

    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.google.guava:guava")
    implementation("org.apache.commons:commons-lang3")

    testImplementation(project(":testkit"))
    testImplementation("org.mockito:mockito-core")
    testRuntimeOnly("org.slf4j:slf4j-log4j12")
    kapt("org.immutables:value")
    compileOnly("org.immutables:value-annotations")
}
