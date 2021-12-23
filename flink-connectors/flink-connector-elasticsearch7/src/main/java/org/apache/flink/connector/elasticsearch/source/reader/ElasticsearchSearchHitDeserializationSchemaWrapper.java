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

package org.apache.flink.connector.elasticsearch.source.reader;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import org.elasticsearch.search.SearchHit;

import java.io.IOException;

public class ElasticsearchSearchHitDeserializationSchemaWrapper<T>
        implements ElasticsearchSearchHitDeserializationSchema<T> {

    private final ElasticsearchSearchHitDeserializationSchema<T> deserializationSchema;

    public ElasticsearchSearchHitDeserializationSchemaWrapper(
            ElasticsearchSearchHitDeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        deserializationSchema.open(context);
    }

    @Override
    public void deserialize(SearchHit record, Collector<T> out) throws IOException {
        deserializationSchema.deserialize(record, out);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
