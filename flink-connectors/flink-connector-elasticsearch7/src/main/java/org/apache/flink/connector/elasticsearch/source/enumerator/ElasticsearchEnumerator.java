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

package org.apache.flink.connector.elasticsearch.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.elasticsearch.common.ElasticsearchUtil;
import org.apache.flink.connector.elasticsearch.common.NetworkClientConfig;
import org.apache.flink.connector.elasticsearch.source.ElasticsearchSourceConfiguration;
import org.apache.flink.connector.elasticsearch.source.split.ElasticsearchSplit;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** The enumerator class for ElasticsearchSource. */
@Internal
public class ElasticsearchEnumerator
        implements SplitEnumerator<ElasticsearchSplit, ElasticsearchEnumState> {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchEnumerator.class);

    private final ElasticsearchSourceConfiguration sourceConfiguration;

    private final NetworkClientConfig networkClientConfig;

    private final SplitEnumeratorContext<ElasticsearchSplit> context;

    private ArrayList<ElasticsearchSplit> splits;

    private RestHighLevelClient client;

    public ElasticsearchEnumerator(
            ElasticsearchSourceConfiguration sourceConfiguration,
            NetworkClientConfig networkClientConfig,
            SplitEnumeratorContext<ElasticsearchSplit> context) {
        this(sourceConfiguration, networkClientConfig, context, Collections.emptySet());
    }

    public ElasticsearchEnumerator(
            ElasticsearchSourceConfiguration sourceConfiguration,
            NetworkClientConfig networkClientConfig,
            SplitEnumeratorContext<ElasticsearchSplit> context,
            Collection<ElasticsearchSplit> restoredSplits) {
        this.sourceConfiguration = sourceConfiguration;
        this.networkClientConfig = networkClientConfig;
        this.context = context;
        this.splits = new ArrayList<>(restoredSplits);
    }

    @Override
    public void start() {
        client = getRestClient();
        LOG.info("Starting the ElasticsearchSourceEnumerator.");

        if (splits.isEmpty()) {
            context.callAsync(this::initializePointInTime, this::handlePointInTimeCreation);
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String hostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        final String hostInfo =
                hostname == null ? "(no host locality info)" : "(on host '" + hostname + "')";
        LOG.info("Subtask {} {} is requesting a file source split", subtaskId, hostInfo);

        final Optional<ElasticsearchSplit> nextSplit = getNextSplit();
        if (nextSplit.isPresent()) {
            final ElasticsearchSplit split = nextSplit.get();
            context.assignSplit(split, subtaskId);
            LOG.info("Assigned split to subtask {} : {}", subtaskId, split);
        } else {
            context.signalNoMoreSplits(subtaskId);
            LOG.info("No more splits available for subtask {}", subtaskId);
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        LOG.error("Received unrecognized event: {}", sourceEvent);
    }

    @Override
    public void addSplitsBack(List<ElasticsearchSplit> splits, int subtaskId) {
        LOG.debug("ElasticsearchEnumerator adds splits back: {}", splits);
        this.splits.addAll(splits);
    }

    @Override
    public ElasticsearchEnumState snapshotState(long checkpointId) throws Exception {
        return ElasticsearchEnumState.fromCollectionSnapshot(splits);
    }

    // ----------------- private methods -------------------

    private RestHighLevelClient getRestClient() {
        return new RestHighLevelClient(
                ElasticsearchUtil.configureRestClientBuilder(
                        RestClient.builder(sourceConfiguration.getHosts().toArray(new HttpHost[0])),
                        networkClientConfig));
    }

    // This method should only be invoked in the coordinator executor thread.
    private String initializePointInTime() throws IOException {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(sourceConfiguration.getIndex());
        request.keepAlive(
                TimeValue.timeValueMillis(sourceConfiguration.getPitKeepAlive().toMillis()));
        OpenPointInTimeResponse response = client.openPointInTime(request, RequestOptions.DEFAULT);
        String pitId = response.getPointInTimeId();
        LOG.debug("Created point in time (pit) with ID {}", pitId);

        return pitId;
    }

    // This method should only be invoked in the coordinator executor thread.
    private void handlePointInTimeCreation(String pitId, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to create point in time (pit) due to ", t);
        }

        splits =
                IntStream.range(0, sourceConfiguration.getNumberOfSlices())
                        .mapToObj(i -> new ElasticsearchSplit(pitId, i))
                        .collect(Collectors.toCollection(ArrayList::new));
    }

    private Optional<ElasticsearchSplit> getNextSplit() {
        int size = splits.size();
        return size == 0 ? Optional.empty() : Optional.of(splits.remove(size - 1));
    }
}
