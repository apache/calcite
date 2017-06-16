---
layout: docs
title: Protocol Testing
sidebar_title: Protocol Testing
permalink: /docs/protocol_testing.html
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

Building Avatica clients in a variety of languages is one of the primary
goals of Avatica. There are various tools which can help in this process,
but one of the most useful is a reference for how clients interact with
the Avatica server.

## Testing with cURL

A trivial way to interact with an Avatica server is using cURL and the JSON
serialization. The below was tested to work with Avatica 1.10.0:

{% highlight bash %}
#!/usr/bin/env bash

set -u

AVATICA=$1
SQL=$2

CONNECTION_ID="conn-$(whoami)-$(date +%s)"
MAX_ROW_COUNT=100
NUM_ROWS=2
OFFSET=0

echo "Open connection"
openConnectionReq="{\"request\": \"openConnection\",\"connectionId\": \"${CONNECTION_ID}\"}"
# Example of how to set connection properties with info key
# openConnectionReqWithProperties="{\"request\": \"openConnection\",\"connectionId\": \"${CONNECTION_ID}\",\"info\": {\"user\": \"SCOTT\",\"password\": \"TIGER\"}}"
curl -i -w "\n" "$AVATICA" -H "Content-Type: application/json" --data "$openConnectionReq"
echo

echo "Create statement"
STATEMENTRSP=$(curl -s "$AVATICA" -H "Content-Type: application/json" --data "{\"request\": \"createStatement\",\"connectionId\": \"${CONNECTION_ID}\"}")
STATEMENTID=$(echo "$STATEMENTRSP" | jq .statementId)
echo

echo "PrepareAndExecuteRequest"
curl -i -w "\n" "$AVATICA" -H "Content-Type: application/json" --data "{\"request\": \"prepareAndExecute\",\"connectionId\": \"${CONNECTION_ID}\",\"statementId\": $STATEMENTID,\"sql\": \"$SQL\",\"maxRowCount\": ${MAX_ROW_COUNT}, \"maxRowsInFirstFrame\": ${NUM_ROWS}}"
echo

# Loop through all the results
ISDONE=false
while ! $ISDONE; do
  OFFSET=$((OFFSET + NUM_ROWS))
  echo "FetchRequest - Offset=$OFFSET"
  FETCHRSP=$(curl -s "$AVATICA" -H "Content-Type: application/json" --data "{\"request\": \"fetch\",\"connectionId\": \"${CONNECTION_ID}\",\"statementId\": $STATEMENTID,\"offset\": ${OFFSET},\"fetchMaxRowCount\": ${NUM_ROWS}}")
  echo "$FETCHRSP"
  ISDONE=$(echo "$FETCHRSP" | jq .frame.done)
  echo
done

echo "Close statement"
curl -i -w "\n" "$AVATICA" -H "Content-Type: application/json" --data "{\"request\": \"closeStatement\",\"connectionId\": \"${CONNECTION_ID}\",\"statementId\": $STATEMENTID}"
echo

echo "Close connection"
curl -i -w "\n" "$AVATICA" -H "Content-Type: application/json" --data "{\"request\": \"closeConnection\",\"connectionId\": \"${CONNECTION_ID}\"}"
echo
{% endhighlight %}
