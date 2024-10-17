#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# $1 is the sub-project directory
# $2 is the artifact ID
# $3 is the version
function snapshot_upload {
    mvn deploy:deploy-file \
        -DgroupId=org.apache.calcite \
        -DartifactId="$2" \
        -Dversion="$3" \
        -Dpackaging=jar \
        -Dfile="./$1/build/libs/$2-$3.jar" \
        -DgeneratePom=false \
        -DpomFile="./$1/build/publications/$1/pom-default.xml" \
        -DrepositoryId=nexus \
        -Durl=https://nexusrepo.looker.com/repository/maven-snapshots/
}

./gradlew build -x :redis:test && ./gradlew jar && ./gradlew generatePom && (
    VERSION="$(sed -n 's/^calcite\.version=\([^ ]*\).*/\1/p' gradle.properties)-SNAPSHOT"
    snapshot_upload core calcite-core "$VERSION"
    snapshot_upload babel calcite-babel "$VERSION"
    snapshot_upload linq4j calcite-linq4j "$VERSION"
    snapshot_upload testkit calcite-testkit "$VERSION"
    echo
    echo "Done uploading version ${VERSION} to Looker Nexus Snapshots!"
)
