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
CREATE KEYSPACE twissandra_small
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE twissandra_small.users (
    username text PRIMARY KEY,
    password text
);

USE twissandra_small;

INSERT INTO users(username, password) VALUES ('fOGctyIDES','cGfDNvOUWH');
INSERT INTO users(username, password) VALUES ('cWIZrdKQmh','haENHSnBMF');
INSERT INTO users(username, password) VALUES ('lixvTEUaBj','gmDSxlydEL');
INSERT INTO users(username, password) VALUES ('PNexGqHdVE','ZSBNHcIrvC');
INSERT INTO users(username, password) VALUES ('PDKIVoezHs','UINXFlcAod');
INSERT INTO users(username, password) VALUES ('HuGetcsXbQ','fXwYWMaSjc');
INSERT INTO users(username, password) VALUES ('MdHQeWbPjB','QlaxOTioNZ');
INSERT INTO users(username, password) VALUES ('UWzCrfaxQi','EzyQckbKOh');
INSERT INTO users(username, password) VALUES ('JmuhsAaMdw','SQbIaqvzfW');
INSERT INTO users(username, password) VALUES ('nFtPHprNOd','CESzsfTALr');
