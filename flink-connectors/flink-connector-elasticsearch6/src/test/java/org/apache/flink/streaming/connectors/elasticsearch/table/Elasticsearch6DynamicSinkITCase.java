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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.util.DockerImageVersions;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.search.SearchHits;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.apache.flink.streaming.connectors.elasticsearch.table.TestContext.context;

/** IT tests for {@link Elasticsearch6DynamicSink}. */
@Testcontainers
public class Elasticsearch6DynamicSinkITCase extends ElasticsearchDynamicSinkBaseITCase {

    private static final String DOCUMENT_TYPE = "MyType";

    @Override
    String getElasticsearchImageName() {
        return DockerImageVersions.ELASTICSEARCH_6;
    }

    @Override
    ElasticsearchDynamicSinkFactoryBase getDynamicSinkFactory() {
        return new Elasticsearch6DynamicSinkFactory();
    }

    @Override
    GetRequest createGetRequest(String index, String id) {
        return new GetRequest(index, DOCUMENT_TYPE, id);
    }

    @Override
    TestContext getPrefilledTestContext(String index) {
        return context()
                .withOption(Elasticsearch6ConnectorOptions.INDEX_OPTION.key(), index)
                .withOption(
                        Elasticsearch6ConnectorOptions.DOCUMENT_TYPE_OPTION.key(), DOCUMENT_TYPE)
                .withOption(
                        Elasticsearch6ConnectorOptions.HOSTS_OPTION.key(),
                        elasticsearchContainer.getHttpHostAddress());
    }

    @Override
    String getConnectorSql(String index) {
        return String.format("'%s'='%s',\n", "connector", "elasticsearch-6")
                + String.format(
                        "'%s'='%s',\n", Elasticsearch6ConnectorOptions.INDEX_OPTION.key(), index)
                + String.format(
                        "'%s'='%s',\n",
                        Elasticsearch6ConnectorOptions.DOCUMENT_TYPE_OPTION.key(), DOCUMENT_TYPE)
                + String.format(
                        "'%s'='%s'\n",
                        Elasticsearch6ConnectorOptions.HOSTS_OPTION.key(),
                        elasticsearchContainer.getHttpHostAddress());
    }

    @Override
    long getTotalSearchHits(SearchHits hits) {
        return hits.getTotalHits();
    }
}
