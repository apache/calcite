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
CREATE KEYSPACE dtcassandra
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

USE dtcassandra;

CREATE TABLE test_simple (
    f_int int PRIMARY KEY,
    f_uuid uuid,
    f_timeuuid timeuuid,
    f_ascii ascii,
    f_text text,
    f_varchar varchar,
    f_varint varint,
    f_bigint bigint,
    f_double double,
    f_float float,
    f_decimal decimal,
    f_blob blob,
    f_boolean boolean,
    f_date date,
    f_inet inet,
    f_smallint smallint,
    f_time time,
    f_timestamp timestamp,
    f_tinyint tinyint,
    f_duration duration
);

INSERT INTO test_simple(f_int,
                        f_uuid,
                        f_timeuuid,
                        f_ascii,
                        f_text,
                        f_varchar,
                        f_varint,
                        f_bigint,
                        f_double,
                        f_float,
                        f_decimal,
                        f_blob,
                        f_boolean,
                        f_date,
                        f_inet,
                        f_smallint,
                        f_time,
                        f_timestamp,
                        f_tinyint,
                        f_duration) VALUES (0,
                                            123e4567-e89b-12d3-a456-426655440000,
                                            8ac6d1dc-fbeb-11e9-8f0b-362b9e155667,
                                            'abcdefg',
                                            'abcdefg',
                                            'abcdefg',
                                            10,
                                            3000000000,
                                            2.0,
                                            5.1,
                                            2.1,
                                            0x20,
                                            true,
                                            '2015-05-03',
                                            '192.168.0.1',
                                            5,
                                            '13:30:54.234',
                                            '2011-02-03T04:05:00.000+0000',
                                            0,
                                            P0000-00-00T89:09:09);


CREATE TABLE test_counter ( f_counter counter, f_int int PRIMARY KEY );

UPDATE test_counter SET f_counter = f_counter + 1 WHERE f_int = 1;


CREATE TABLE test_collections (
    f_int int PRIMARY KEY,
    f_list list<int>,
    f_map map<text, text>,
    f_set set<double>,
    f_tuple tuple<bigint, blob, timestamp>
);

INSERT INTO test_collections (f_int, f_list, f_map, f_set, f_tuple) VALUES (0,
                                                                            [1,2,3],
                                                                            {'k1':'v1', 'k2':'v2'},
                                                                            {2.0, 3.1},
                                                                            (3000000000, 0x30FF87, '2015-05-03 13:30:54.234'));


CREATE TABLE test_frozen_collections (
    f_int int PRIMARY KEY,
    f_list frozen<list<int>>,
    f_map frozen<map<text, text>>,
    f_set frozen<set<double>>,
    f_tuple frozen<tuple<bigint, blob, timestamp>>
);

INSERT INTO test_frozen_collections (f_int, f_list, f_map, f_set, f_tuple) VALUES (0,
                                                                                   [1,2,3],
                                                                                   {'k1':'v1', 'k2':'v2'},
                                                                                   {2.0, 3.1},
                                                                                   (3000000000, 0x30FF87, '2015-05-03 13:30:54.234'));

CREATE TABLE test_type ( f_user varchar, f_id bigint PRIMARY KEY );

INSERT INTO test_type (f_user, f_id) VALUES ('ANNA', 3000000000);

CREATE TABLE test_date_type ( f_user varchar, f_date date PRIMARY KEY );

INSERT INTO test_date_type (f_user, f_date) VALUES ('ANNA', '2015-05-03');

CREATE TABLE test_timestamp_type ( f_user varchar, f_timestamp timestamp PRIMARY KEY );

INSERT INTO test_timestamp_type (f_user, f_timestamp) VALUES ('ANNA', '2011-02-03T04:05:00.00+0000');
