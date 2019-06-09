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
    agent {
        node {
            label 'git-websites'
        }
    }

    environment {
        COMPOSE_PROJECT_NAME = "${env.JOB_NAME}-${env.BUILD_ID}"
    }

    options {
        timeout(time: 40, unit: 'MINUTES')
    }

    stages {
        stage('Build Site') {
            steps {
                dir ("site") {
                    sh '''docker-compose run -e JEKYLL_UID=$(id -u) -e JEKYLL_GID=$(id -g) build-site
                        docker-compose run -u $(id -u):$(id -g) generate-javadoc
                        cd target/
                        ls -la
                    '''
                }
                echo "Website assets and javadoc built."
            }
        }

        stage('Deploy Site') {
            steps {
                echo 'Deploying to git repository'
                dir ("site") {
                    sh '''
                        shopt -s extglob

                        git clone --depth 1 --branch asf-site https://gitbox.apache.org/repos/asf/calcite-site.git deploy
                        cd deploy/

                        rm -rf !(avatica)
                        cp -r ../target/* .

                        git add .
                        git commit -m "Automatic site deployment at $GIT_COMMIT"
                        git push origin HEAD:asf-site
                    '''
                }
                echo "Deployment completed."
            }
        }
    }
    post {
        always {
            sh 'docker system prune --force --all'
        }
    }
}