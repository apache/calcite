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

--
-- Testing data file is generated in MySQL 5.7.27
--

SET @@session.time_zone = "+00:00";

DROP TABLE IF EXISTS `test_types`;
CREATE TABLE `test_types`
(`id` int(11) NOT NULL AUTO_INCREMENT,
`f_tinyint` TINYINT NOT NULL,
`f_smallint` SMALLINT NOT NULL,
`f_mediumint` MEDIUMINT NOT NULL,
`f_int` INT(11) NOT NULL,
`f_bigint` BIGINT(20) NOT NULL,
`f_datetime` DATETIME NOT NULL,
`f_timestamp` TIMESTAMP NOT NULL,
`f_time` TIME NOT NULL,
`f_year` YEAR NOT NULL,
`f_date` DATE NOT NULL,
`f_float` FLOAT NOT NULL ,
`f_double` DOUBLE NOT NULL,
`f_decimal1` DECIMAL(6) NOT NULL,
`f_decimal2` DECIMAL(10, 5) NOT NULL,
`f_decimal3` DECIMAL(12, 0) NOT NULL,
`f_decimal4` DECIMAL(6, 3) NOT NULL,
`f_decimal5` DECIMAL NOT NULL,
`f_decimal6` DECIMAL(30,25),
`f_varchar` VARCHAR(32) NOT NULL,
`f_varchar_overflow` VARCHAR(5000) NOT NULL,
`f_varchar_null` VARCHAR(15),
`f_char_32` CHAR(32) NOT NULL,
`f_char_255` CHAR(255) NOT NULL,
`f_char_null` CHAR(1),
`f_boolean` BOOLEAN NOT NULL,
`f_bool` BOOL NOT NULL,
`f_tinytext` TINYTEXT NOT NULL,
`f_text` TEXT NOT NULL,
`f_mediumtext` MEDIUMTEXT NOT NULL,
`f_longtext` LONGTEXT NOT NULL,
`f_tinyblob` TINYBLOB NOT NULL,
`f_blob` BLOB NOT NULL,
`f_mediumblob` MEDIUMBLOB NOT NULL,
`f_longblob` LONGBLOB NOT NULL,
`f_varbinary` VARBINARY(32) NOT NULL,
`f_varbinary_overflow` VARBINARY(5000) NOT NULL,
`f_enum` ENUM('MYSQL','Hello','world','computer') NOT NULL,
`f_set` SET ('a','b','c','d','e','f','g','h','i','j','k','l', 'm','n','o','p','q','r','s','t','u','v','w','x', 'y','z') NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `test_types` VALUES
(
1,
100,
10000,
1000000,
10000000,
100000000000,
'2019-10-02 10:59:59',
'1988-11-23 22:10:08',
'00:36:52',
2012,
'2020-01-29',
0.987654321,
1234567890.12345,
123456.123456789,
12345.67890,
12345678901,
123.10,
12345.678,
12345.1234567890123456789012345,
concat(char(97+(2 % 26)), REPEAT('x', 31)),
concat(char(97+(2 % 26)), REPEAT('データ', 300)),
NULL,
concat(char(97+(2 % 26)), REPEAT('данные', 2)),
concat(char(97+(2 % 26)), REPEAT('数据', 100)),
NULL,
FALSE,
TRUE,
concat(char(97+(2 % 26)), REPEAT('Data', 50)),
concat(char(97+(2 % 26)), REPEAT('Daten', 200)),
concat(char(97+(2 % 26)), REPEAT('Datos', 200)),
concat(char(97+(2 % 26)), REPEAT('Les données', 800)),
concat(char(97+(2 % 26)), REPEAT(0x0a, 100)),
concat(char(97+(2 % 26)), REPEAT(0x0b, 400)),
concat(char(97+(2 % 26)), REPEAT(0x0c, 800)),
concat(char(97+(2 % 26)), REPEAT(0x0d, 1000)),
concat(char(97+(2 % 26)), REPEAT(0x0e, 8)),
concat(char(97+(2 % 26)), REPEAT(0xff, 100)),
'MYSQL',
'z'
);

INSERT INTO `test_types` VALUES
(
2,
-100,
-10000,
-1000000,
-10000000,
-9223372036854775807,
'2255-01-01 12:12:12',
'2020-01-01 00:00:00',
'23:11:00',
0000,
'1970-01-01',
-12345678.1234,
-1234567890.123456,
9.12345678,
-567.8910,
987654321.05,
456.000,
0.000,
-0.0123456789012345678912345,
concat(char(97+(3 % 26)), REPEAT('y', 31)),
concat(char(97+(3 % 26)), REPEAT('データ', 300)),
NULL,
concat(char(97+(3 % 26)), REPEAT('данные', 2)),
concat(char(97+(3 % 26)), REPEAT('数据', 100)),
NULL,
FALSE,
TRUE,
concat(char(97+(3 % 26)), REPEAT('Data', 50)),
concat(char(97+(3 % 26)), REPEAT('Daten', 200)),
concat(char(97+(3 % 26)), REPEAT('Datos', 200)),
concat(char(97+(3 % 26)), REPEAT('Les données', 800)),
concat(char(97+(3 % 26)), REPEAT(0x0a, 100)),
concat(char(97+(3 % 26)), REPEAT(0x0b, 400)),
concat(char(97+(3 % 26)), REPEAT(0x0c, 800)),
concat(char(97+(3 % 26)), REPEAT(0x0d, 1000)),
concat(char(97+(3 % 26)), REPEAT(0x0e, 8)),
concat(char(97+(3 % 26)), REPEAT(0xff, 100)),
2,
'a,e,i,o,u'
);

# End data_types.sql
