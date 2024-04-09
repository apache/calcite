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

    implementation("com.google.guava:guava")
    implementation("org.apache.arrow:arrow-memory-netty")
    implementation("org.apache.arrow:arrow-vector")
    implementation("org.apache.arrow.gandiva:arrow-gandiva")
    annotationProcessor("org.immutables:value")
    compileOnly("org.immutables:value-annotations")

    testImplementation("org.apache.arrow:arrow-jdbc")
    testImplementation("net.hydromatic:scott-data-hsqldb")
    testImplementation("org.apache.commons:commons-lang3")
    testImplementation(project(":core"))
    testImplementation(project(":testkit"))
}
