#!/bin/bash
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

# This file is called automatically by Vagrant VM provision

echo Importing zips dataset
if [[ ! -f /tmp/zips.json ]]; then
  curl -s -o /tmp/zips.json http://media.mongodb.org/zips.json
fi
mongoimport --db test --collection zips --file /tmp/zips.json

echo Importing foodmart dataset
if [[ ! -f /tmp/foodmart-json.jar ]]; then
  curl -s -o /tmp/foodmart-json.jar http://nexus.pentaho.org/content/groups/omni/pentaho/mondrian-data-foodmart-json/0.3.3/mondrian-data-foodmart-json-0.3.3.jar
fi

unzip -u /tmp/foodmart-json.jar -d /tmp/mongodb-foodmart
cd /tmp/mongodb-foodmart
for i in *.json; do
  echo .. importing $i
  mongoimport --db foodmart --collection ${i/.json/} --file $i
done

echo Importing additional test collections
mongo test <<JSON
  db.createCollection("datatypes")
  db.datatypes.insert({
    "_id" : ObjectId("53655599e4b0c980df0a8c27"),
    "_class" : "com.ericblue.Test",
    "date" : ISODate("2012-09-05T07:00:00Z"),
    "value" : 1231,
    "ownerId" : "531e7789e4b0853ddb861313"
  })
JSON
