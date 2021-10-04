/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.streaming.api.functions.source;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/**
 * ITCases for {@link org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperator}.
 */
public class ContinuousFileReaderOperatorITCase {
    @Rule public TemporaryFolder temp = new TemporaryFolder();

    /** Tests https://issues.apache.org/jira/browse/FLINK-20888. */

    /*
    @Test
    public void testChainedOperatorsAreNotPrematurelyClosed() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        File input = temp.newFile("input");
        FileUtils.write(input, "test", StandardCharsets.UTF_8);
        DataStream<String> stream = env.readTextFile(input.getAbsolutePath());
        final FileSink<String> sink =
                FileSink.forRowFormat(
                                new Path(temp.newFolder("output").getAbsolutePath()),
                                new SimpleStringEncoder<String>())
                        .withOutputFileConfig(OutputFileConfig.builder().build())
                        .withRollingPolicy(
                                DefaultRollingPolicy.builder().withMaxPartSize(1024 * 1024).build())
                        .build();
        stream.sinkTo(sink);
        env.execute("test");
    }

     */
}
