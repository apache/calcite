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

node('ubuntu') {
  def JAVA_JDK_17=tool name: 'jdk_17_latest', type: 'hudson.model.JDK'
  stage('Checkout') {
    if(env.CHANGE_ID) {
      // By default checkout on PRs will fetch only that branch and nothing else. However, in order for the Sonar plugin
      // to function properly we need to fetch also the target branch ${CHANGE_TARGET} so we need to customize the
      // refspec. If the target branch of the PR is not present warnings like the following may appear:
      // Could not find ref 'main' in refs/heads, refs/remotes/upstream or refs/remotes/origin
      checkout([
        $class: 'GitSCM',
        branches: scm.branches,
        doGenerateSubmoduleConfigurations: scm.doGenerateSubmoduleConfigurations,
        extensions: scm.extensions,
        userRemoteConfigs: scm.userRemoteConfigs + [[
          name: 'origin',
          refspec: scm.userRemoteConfigs[0].refspec+ " +refs/heads/${CHANGE_TARGET}:refs/remotes/origin/${CHANGE_TARGET}",
          url: scm.userRemoteConfigs[0].url,
          credentialsId: scm.userRemoteConfigs[0].credentialsId
        ]],
      ])
    } else {
      checkout scm
    }
  }
  stage('Code Quality') {
    timeout(time: 1, unit: 'HOURS') {
      // The following option `--add-opens=java.base/java.nio=ALL-UNNAMED` is required jdk17+
      // to avoid error. See https://arrow.apache.org/docs/java/install.html#java-compatibility
      withEnv(["Path+JDK=$JAVA_JDK_17/bin","JAVA_HOME=$JAVA_JDK_17","_JAVA_OPTIONS=--add-opens=java.base/java.nio=ALL-UNNAMED"]) {
        withCredentials([string(credentialsId: 'SONARCLOUD_TOKEN', variable: 'SONAR_TOKEN')]) {
          if ( env.BRANCH_NAME.startsWith("PR-") ) {
            sh './gradlew --no-parallel --no-daemon jacocoAggregateTestReport sonar -PenableJacoco -Dsonar.pullrequest.branch=${CHANGE_BRANCH} -Dsonar.pullrequest.base=${CHANGE_TARGET} -Dsonar.pullrequest.key=${CHANGE_ID} -Dsonar.login=${SONAR_TOKEN}'
          } else {
            sh './gradlew --no-parallel --no-daemon jacocoAggregateTestReport sonar -PenableJacoco -Dsonar.branch.name=${BRANCH_NAME} -Dsonar.login=${SONAR_TOKEN}'
          }
        }
      }
    }
  }
  cleanWs()
}

