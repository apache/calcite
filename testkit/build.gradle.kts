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
}

dependencies {
    api(project(":core"))
    api("org.checkerframework:checker-qual")

    implementation(platform("org.junit:junit-bom"))
    implementation(kotlin("stdlib-jdk8"))
    implementation("net.hydromatic:quidem")
    implementation("net.hydromatic:foodmart-data-hsqldb")
    implementation("net.hydromatic:foodmart-queries")
    implementation("net.hydromatic:scott-data-hsqldb")
    implementation("net.hydromatic:steelwheels-data-hsqldb")
    implementation("org.apache.commons:commons-dbcp2")
    implementation("org.apache.commons:commons-lang3")
    implementation("org.apache.commons:commons-pool2")
    implementation("org.hamcrest:hamcrest")
    implementation("org.hsqldb:hsqldb::jdk8")
    annotationProcessor("org.immutables:value")
    compileOnly("org.immutables:value-annotations")
    implementation("org.incava:java-diff")
    implementation("org.junit.jupiter:junit-jupiter")

    testImplementation(kotlin("test"))
    testImplementation(kotlin("test-junit5"))
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    kotlinOptions {
        jvmTarget = "1.8"
    }
}
