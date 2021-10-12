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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSink} that describes how to create a {@link ElasticsearchSink} from a
 * logical description.
 */
@Internal
abstract class ElasticsearchDynamicSinkBase implements DynamicTableSink {

    final EncodingFormat<SerializationSchema<RowData>> format;
    final DataType physicalRowDataType;
    final List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex;
    final ElasticsearchConfiguration config;

    ElasticsearchDynamicSinkBase(
            EncodingFormat<SerializationSchema<RowData>> format,
            ElasticsearchConfiguration config,
            List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex,
            DataType physicalRowDataType) {
        this.format = checkNotNull(format);
        this.physicalRowDataType = checkNotNull(physicalRowDataType);
        this.primaryKeyLogicalTypesWithIndex = checkNotNull(primaryKeyLogicalTypesWithIndex);
        this.config = checkNotNull(config);
    }

    Function<RowData, String> createKeyExtractor() {
        return KeyExtractor.createKeyExtractor(
                primaryKeyLogicalTypesWithIndex, config.getKeyDelimiter());
    }

    IndexGenerator createIndexGenerator() {
        return IndexGeneratorFactory.createIndexGenerator(
                config.getIndex(),
                DataType.getFieldNames(physicalRowDataType),
                DataType.getFieldDataTypes(physicalRowDataType));
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public int hashCode() {
        return Objects.hash(format, physicalRowDataType, primaryKeyLogicalTypesWithIndex, config);
    }
}
