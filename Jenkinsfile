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

pipeline {
    agent { docker { image 'openjdk:17-alpine' } }
    stages {
      stage('Checkout') {
        checkout scm
      }
      stage('Code Quality') {
        steps {
          echo 'Checking Code Quality on SonarCloud'
          if ( env.BRANCH_NAME.startsWith("PR-") ) {
            sonarcloudParams="-Dsonar.pullrequest.branch=${CHANGE_BRANCH} -Dsonar.pullrequest.base=${CHANGE_TARGET} -Dsonar.pullrequest.key=${CHANGE_ID}"
          } else {
            sonarcloudParams="-Dsonar.branch.name=${BRANCH_NAME}"
          }
          withCredentials([string(credentialsId: 'SONARCLOUD_TOKEN', variable: 'SONAR_TOKEN')]) {
              sh './gradlew --no-parallel --no-daemon build jacocoTestReport sonar -PenableJacoco -Dsonar.login=${SONAR_TOKEN}'
        }
      }
    }
}

