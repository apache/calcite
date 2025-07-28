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
    implementation(project(":core"))
    implementation(project(":linq4j"))
    implementation("org.apache.arrow:arrow-vector")
    implementation("org.apache.arrow:arrow-memory-netty")
    // Gandiva available for future vectorized expression evaluation
    implementation("org.apache.arrow.gandiva:arrow-gandiva")
    implementation("com.opencsv:opencsv:5.7.1")
    implementation("org.slf4j:slf4j-api")

    testImplementation(project(":testkit"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
}

tasks.withType<JavaCompile> {
    options.compilerArgs.addAll(listOf("-Xlint:none", "-Werror"))
    options.compilerArgs.remove("-Werror")
    options.isWarnings = false
}
