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
    `maven-publish`
    id("calcite.local-maven-repo")
}

publishing {
    publications {
        withType<MavenPublication>().configureEach {
            pom {
                name.set(artifactId)
                description.set(providers.provider { project.description })
                // It takes value from root project always: https://github.com/gradle/gradle/issues/13302
                inceptionYear.set("2012")
                url.set("https://calcite.apache.org")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                        comments.set("A business-friendly OSS license")
                        distribution.set("repo")
                    }
                }
                issueManagement {
                    system.set("Jira")
                    url.set("https://issues.apache.org/jira/browse/CALCITE")
                }
                mailingLists {
                    mailingList {
                        name.set("Apache Calcite developers list")
                        subscribe.set("dev-subscribe@calcite.apache.org")
                        unsubscribe.set("dev-unsubscribe@calcite.apache.org")
                        post.set("dev@calcite.apache.org")
                        archive.set("https://lists.apache.org/list.html?dev@calcite.apache.org")
                    }
                }
                scm {
                    connection.set("scm:git:https://gitbox.apache.org/repos/asf/calcite.git")
                    developerConnection.set("scm:git:https://gitbox.apache.org/repos/asf/calcite.git")
                    url.set("https://github.com/apache/calcite")
                    tag.set("HEAD")
                }
            }
        }
    }
}
