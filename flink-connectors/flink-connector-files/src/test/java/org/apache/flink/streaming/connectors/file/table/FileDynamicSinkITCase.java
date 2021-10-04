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

package org.apache.flink.streaming.connectors.file.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.util.FileUtils;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;

/** Test for {@link FileDynamicSink}. */
public class FileDynamicSinkITCase {

    @Test
    public void testExceptionWhenSettingParallelismWithUpdatingQuery() {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final String testSourceTableName = "test_source_table";
        tEnv.executeSql(buildSourceTableSql(testSourceTableName, false));

        final String testSinkTableName = "test_sink_table";
        tEnv.executeSql(buildSinkTableSql(testSinkTableName, 10, false));
        String sql =
                String.format(
                        "INSERT INTO %s SELECT DISTINCT * FROM %s",
                        testSinkTableName, testSourceTableName);

        assertThrows(
                "filesystem sink doesn't support setting parallelism (10) by 'sink.parallelism' when the input stream is not INSERT only.",
                ValidationException.class,
                () -> tEnv.explainSql(sql));
    }

    @Test
    public void testFileSystemTableSinkWithParallelismInStreaming() throws IOException {
        final int parallelism = 5;
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 8);

        final String testSourceTableName = "test_source_table";
        tEnv.executeSql(buildSourceTableSql(testSourceTableName, false));

        // verify operator parallelisms when compaction is not enabled
        final String testSinkTableName = "test_sink_table";
        tEnv.executeSql(buildSinkTableSql(testSinkTableName, parallelism, false));
        final String sql0 = buildInsertIntoSql(testSinkTableName, testSourceTableName);
        final String actualNormal = tEnv.explainSql(sql0, ExplainDetail.JSON_EXECUTION_PLAN);
        final String expectedNormal =
                FileUtils.readFileUtf8(
                        new File(
                                "/explain/filesystem/testFileSystemTableSinkWithParallelismInStreaming.out"));
        System.out.println(expectedNormal);
        /* TODO
                readFromResource(
                        "/explain/filesystem/testFileSystemTableSinkWithParallelismInStreaming.out");
        assertEquals(
                replaceStreamNodeId(replaceStageId(expectedNormal)),
                replaceStreamNodeId(replaceStageId(actualNormal)));

         */

    }

    @Test
    public void testFileSystemTableSinkWithParallelismInBatch() {
        final int parallelism = 5;
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 8);

        final String testSourceTableName = "test_source_table";
        final String testSinkTableName = "test_sink_table";
        tEnv.executeSql(buildSourceTableSql(testSourceTableName, true));
        tEnv.executeSql(buildSinkTableSql(testSinkTableName, parallelism, false));

        final String sql = buildInsertIntoSql(testSinkTableName, testSourceTableName);
        final String actual = tEnv.explainSql(sql, ExplainDetail.JSON_EXECUTION_PLAN);
        /* TODO
        final String expected =
                readFromResource(
                        "/explain/filesystem/testFileSystemTableSinkWithParallelismInBatch.out");

        assertEquals(
                replaceStreamNodeId(replaceStageId(expected)),
                replaceStreamNodeId(replaceStageId(actual)));

         */
    }

    private static String buildSourceTableSql(String testSourceTableName, boolean bounded) {
        return String.format(
                "CREATE TABLE %s ("
                        + " id BIGINT,"
                        + " real_col FLOAT,"
                        + " double_col DOUBLE,"
                        + " decimal_col DECIMAL(10, 4)"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'bounded' = '%s'"
                        + ")",
                testSourceTableName, bounded);
    }

    private static String buildSinkTableSql(
            String tableName, int parallelism, boolean autoCompaction) {
        return String.format(
                "CREATE TABLE %s ("
                        + " id BIGINT,"
                        + " real_col FLOAT,"
                        + " double_col DOUBLE,"
                        + " decimal_col DECIMAL(10, 4)"
                        + ") WITH ("
                        + " 'connector' = 'filesystem',"
                        + " 'path' = '/tmp',"
                        + " 'format' = 'testcsv',"
                        + " 'sink.parallelism' = '%s'"
                        + ")",
                tableName, autoCompaction, parallelism);
    }

    private static String buildInsertIntoSql(String sinkTable, String sourceTable) {
        return String.format("INSERT INTO %s SELECT * FROM %s", sinkTable, sourceTable);
    }
}
