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

package org.apache.flink.connector.elasticsearch.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Objects;

/** A {@link SourceSplit} for an Elasticsearch 'slice'. */
public class ElasticsearchSplit implements SourceSplit {
    private final String pitId;
    private final int sliceId;

    public ElasticsearchSplit(String pitId, int sliceId) {
        this.pitId = pitId;
        this.sliceId = sliceId;
    }

    public String getPitId() {
        return pitId;
    }

    public int getSliceId() {
        return sliceId;
    }

    @Override
    public String splitId() {
        return pitId + "-" + sliceId;
    }

    @Override
    public String toString() {
        return String.format("[PitId: %s, SliceId: %d]", pitId, sliceId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ElasticsearchSplit that = (ElasticsearchSplit) o;
        return sliceId == that.sliceId && Objects.equals(pitId, that.pitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pitId, sliceId);
    }
}
