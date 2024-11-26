/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.commons.lang3.tuple.Pair;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class JdbctesIT extends AbstractJdbcIT {

    private static final String TEST_IMAGE = "trinodb/trino:445";
    private static final String TEST_CONTAINER_HOST = "e2e_trinodb";
    //private static final String HOST = "seawave1.fyre.ibm.com";
    private static final String TEST_SCHEMA = "default";
    private static final String TEST_SOURCE = "e2e_table_source";
    private static final String TEST_SINK = "e2e_table_sink";
    private static final String TEST_USERNAME = "root";
    private static final String TEST_PASSWORD = "";
    private static final int TEST_PORT = 8080;
    private static final String TEST_URL = "jdbc:trino://seawave1.fyre.ibm.com:8080/memory/default";


    private static final String DRIVER_CLASS = "io.trino.jdbc.TrinoDriver";

    /*private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_TEST_source_and_sink.conf");*/
    private static final String CREATE_SQL =
            "create table if not exists %s"
                    + "(\n"
                    + "    TEST_BIGINT            BIGINT,\n"
                    + "    TEST_VARCHAR          VARCHAR\n"
                    + ")";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        //String jdbcUrl = String.format(TEST_URL, TEST_PORT);
        String jdbcUrl = String.format(TEST_URL);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(TEST_SCHEMA, TEST_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(TEST_IMAGE)
              //  .networkAliases(TEST_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                //.port(TEST_PORT)
                //.localPort(TEST_PORT)
                .jdbcTemplate(TEST_URL)
                .jdbcUrl(jdbcUrl)
                .userName(TEST_USERNAME)
                .password(TEST_PASSWORD)
                .database(TEST_SCHEMA)
                .sourceTable(TEST_SOURCE)
                .sinkTable(TEST_SINK)
                .createSql(CREATE_SQL)
                //.configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .tablePathFullName(String.format("%s.%s", TEST_SCHEMA, TEST_SOURCE))
                .build();
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/io/trino/trino-jdbc/445/trino-jdbc-445.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                        "TEST_BIGINT",
                        "TEST_VARCHAR"
                        };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                    i % 2 == 0 ? (byte) 1 : (byte) 0,
                                    i,
                                    i,
                                    i,
                                    Short.valueOf("1"),
                                    Byte.valueOf("1"),
                                    i,
                                    Long.parseLong("1"),
                                    BigDecimal.valueOf(i, 0),
                                    BigDecimal.valueOf(i, 18),
                                    BigDecimal.valueOf(i, 18),
                                    BigDecimal.valueOf(i, 18),
                                    Float.parseFloat("1.1"),
                                    Float.parseFloat("1.1"),
                                    Double.parseDouble("1.1"),
                                    Double.parseDouble("1.1"),
                                    'f',
                                    'f',
                                    String.format("f1_%s", i),
                                    String.format("f1_%s", i),
                                    String.format("f1_%s", i),
                                    String.format("{\"aa\":\"bb_%s\"}", i),
                                    String.format("f1_%s", i),
                                    String.format("f1_%s", i),
                                    Timestamp.valueOf(LocalDateTime.now()),
                                    new Timestamp(System.currentTimeMillis()),
                                    Date.valueOf(LocalDate.now()),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    protected String buildTableInfoWithSchema(String catalog, String schema, String table) {
        return buildTableInfoWithSchema(schema, table);
    }

    protected void clearTable(String catalog, String schema, String table) {
        clearTable(schema, table);
    }

    @Override
    protected GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(TEST_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(TEST_CONTAINER_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(TEST_IMAGE)));
        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", 5236, 8080)));

        return container;
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }
}
