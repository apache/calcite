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
import com.github.vlsi.gradle.publishing.dsl.simplifyXml
import com.github.vlsi.gradle.publishing.dsl.versionFromResolution

plugins {
    id("calcite.reproducible-builds")
    id("calcite.java-library")
    id("calcite.maven-publish")
}

java {
    withJavadocJar()
    withSourcesJar()
}

dependencies {
    // If the user adds core and api with different versions,
    // then Gradle would select **both** core and api with the same version
    // Note: un-comment when testng-bom is published
    // implementation(platform(project(":testng-bom")))
    // For some reason this can't be in code-quality/calcite.testing :(
    testImplementation(project(":testng-test-kit"))
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            // Gradle feature variants can't be mapped to Maven's pom
            suppressAllPomMetadataWarnings()

            // Use the resolved versions in pom.xml
            // Gradle might have different resolution rules, so we set the versions
            // that were used in Gradle build/test.
            versionFromResolution()

            pom {
                simplifyXml()
            }
        }
    }
}
