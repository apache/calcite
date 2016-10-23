#! /usr/bin/env bash

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

# This script will regenerate the protobuf code for Avatica. Slightly
# modified script over one in Accumulo.

# NOTES:
#   To support this script being called by other modules, only edit the right side.
#   In other scripts, set the variables that diverge from the defaults below, then call this script.
#   Leave the BUILD_DIR and FINAL_DIR alone for Maven builds.
# ========================================================================================================================
[[ -z $REQUIRED_PROTOC_VERSION ]] && REQUIRED_PROTOC_VERSION='libprotoc 3.1.0'
[[ -z $BUILD_DIR ]]               && BUILD_DIR='target/proto-tmp'
[[ -z $FINAL_DIR ]]               && FINAL_DIR='src/main'
# ========================================================================================================================

fail() {
  echo "$@"
  exit 1
}

# Test to see if we have protoc installed
VERSION=$(protoc --version 2>/dev/null | grep -F "${REQUIRED_PROTOC_VERSION}" |  wc -l)
if [[ $VERSION -ne 1 ]] ; then
  # Nope: bail
  echo "****************************************************"
  echo "*** protoc is not available"
  echo "***   expecting 'protoc --version' to return ${REQUIRED_PROTOC_VERSION}"
  echo "*** generated code will not be updated"
  fail "****************************************************"
fi

# Ensure output directories are created
PROTOC_ARGS="-I src/main/protobuf --java_out=$BUILD_DIR"
rm -rf $BUILD_DIR
mkdir -p $BUILD_DIR

protoc ${PROTOC_ARGS} src/main/protobuf/*.proto || fail unable to generate Java protocol buffer classes

# For all generated protobuf code, suppress all warnings and add the LICENSE header
s='@SuppressWarnings({"unused", "rawtypes"})'
find $BUILD_DIR -name '*.java' -print0 | xargs -0 sed -i.orig -e 's/\(public final class \)/'"$s"' \1/;s/ *$//'

PREFIX="/*
"
LINE_NOTATION=" *"
SUFFIX="
 */"
FILE_SUFFIX=(.java)

for file in "${FILE_SUFFIX[@]}"; do
  for f in $(find $BUILD_DIR/ -name "*$file"); do
    cat - "$f" > "${f}-with-license" <<EOF
${PREFIX}${LINE_NOTATION} Licensed to the Apache Software Foundation (ASF) under one or more
${LINE_NOTATION} contributor license agreements.  See the NOTICE file distributed with
${LINE_NOTATION} this work for additional information regarding copyright ownership.
${LINE_NOTATION} The ASF licenses this file to you under the Apache License, Version 2.0
${LINE_NOTATION} (the "License"); you may not use this file except in compliance with
${LINE_NOTATION} the License.  You may obtain a copy of the License at
${LINE_NOTATION}
${LINE_NOTATION} http://www.apache.org/licenses/LICENSE-2.0
${LINE_NOTATION}
${LINE_NOTATION} Unless required by applicable law or agreed to in writing, software
${LINE_NOTATION} distributed under the License is distributed on an "AS IS" BASIS,
${LINE_NOTATION} WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
${LINE_NOTATION} See the License for the specific language governing permissions and
${LINE_NOTATION} limitations under the License.${SUFFIX}
EOF
  done
done

# For every generated java file, compare it with the version-controlled one, and copy the ones that have changed into place
SDIR="${BUILD_DIR}/org/apache/calcite/avatica/proto"
DDIR="${FINAL_DIR}/java/org/apache/calcite/avatica/proto"
FILE_SUFFIX=(.java)
mkdir -p "$DDIR"
for file in "${FILE_SUFFIX[@]}"; do
  for f in $(find $SDIR -name *$file); do
    DEST=$DDIR/$(basename "$f")
    if ! cmp -s "${f}-with-license" "${DEST}" ; then
      echo cp -f "${f}-with-license" "${DEST}"
      cp -f "${f}-with-license" "${DEST}" || fail unable to copy files to java workspace
    fi
  done
done
