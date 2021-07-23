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
val sqllineClasspath by configurations.creating {
    isCanBeConsumed = false
    extendsFrom(configurations.testRuntimeClasspath.get())
}

dependencies {
    api(project(":core"))
    api(project(":file"))
    api(project(":linq4j"))
    api("org.checkerframework:checker-qual")

    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.google.guava:guava")
    implementation("org.apache.calcite.avatica:avatica-core")

    testImplementation("sqlline:sqlline")
    testImplementation(project(":core", "testClasses"))

    sqllineClasspath(project(":example:csv", "testClasses"))
}

val buildSqllineClasspath by tasks.registering(Jar::class) {
    inputs.files(sqllineClasspath).withNormalizer(ClasspathNormalizer::class.java)
    archiveFileName.set("sqllineClasspath.jar")
    manifest {
        attributes(
            "Main-Class" to "sqlline.SqlLine",
            "Class-Path" to provider {
                // Class-Path is a list of URLs
                sqllineClasspath.joinToString(" ") {
                    it.toURI().toURL().toString()
                }
            }
        )
    }
}
