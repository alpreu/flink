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

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.elasticsearch.source.ElasticsearchSource;
import org.apache.flink.connector.elasticsearch.source.ElasticsearchSourceBuilder;
import org.apache.flink.connector.elasticsearch.source.reader.ElasticsearchSearchHitDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import org.apache.http.HttpHost;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A Elasticsearch {@link ScanTableSource}. */
@Internal
public class ElasticsearchDynamicSource implements ScanTableSource {

    /** Data type that describes the final output of the source. */
    private final DataType producedDataType;

    /** Data type to configure the format. */
    private final DataType physicalDataType;

    /** Scan format for decoding records from Elasticsearch. */
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    private final ElasticsearchSourceConfig sourceConfig;

    private final String tableIdentifier;

    public ElasticsearchDynamicSource(
            DataType physicalDataType,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            ElasticsearchSourceConfig sourceConfig,
            String tableIdentifier) {
        this(physicalDataType, physicalDataType, decodingFormat, sourceConfig, tableIdentifier);
    }

    public ElasticsearchDynamicSource(
            DataType physicalDataType,
            DataType producedDataType,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            ElasticsearchSourceConfig sourceConfig,
            String tableIdentifier) {
        this.physicalDataType = checkNotNull(physicalDataType);
        this.producedDataType = checkNotNull(producedDataType);
        this.decodingFormat = checkNotNull(decodingFormat);
        this.sourceConfig = checkNotNull(sourceConfig);
        this.tableIdentifier = checkNotNull(tableIdentifier);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {

        final DeserializationSchema<RowData> deserialization =
                createDeserialization(context, decodingFormat);

        final TypeInformation<RowData> producedTypeInformation =
                context.createTypeInformation(producedDataType);

        final ElasticsearchSource<RowData> elasticsearchSource =
                createElasticsearchSource(deserialization, producedTypeInformation);

        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
                return execEnv.fromSource(
                        elasticsearchSource,
                        WatermarkStrategy.noWatermarks(),
                        "ElasticsearchSource-" + tableIdentifier);
            }

            @Override
            public boolean isBounded() {
                return elasticsearchSource.getBoundedness() == Boundedness.BOUNDED;
            }
        };
    }

    ElasticsearchSource<RowData> createElasticsearchSource(
            DeserializationSchema<RowData> deserializationSchema,
            TypeInformation<RowData> producedTypeInformation) {
        final ElasticsearchSearchHitDeserializationSchema<RowData> elasticsearchDeserializer =
                createElasticsearchDeserializationSchema(
                        deserializationSchema, producedTypeInformation);

        final ElasticsearchSourceBuilder<RowData> builder = ElasticsearchSource.builder();

        builder.setDeserializationSchema(elasticsearchDeserializer);
        // TODO: add NetworkClientConfig options here
        builder.setHosts(sourceConfig.getHosts().toArray(new HttpHost[0]));
        builder.setIndexName(sourceConfig.getIndex());
        builder.setNumberOfSearchSlices(sourceConfig.getNumberOfSlices());
        builder.setPitKeepAlive(sourceConfig.getPitKeepAlive());

        return builder.build();
    }

    private ElasticsearchSearchHitDeserializationSchema<RowData>
            createElasticsearchDeserializationSchema(
                    DeserializationSchema<RowData> deserializationSchema,
                    TypeInformation<RowData> producedTypeInformation) {
        return new DynamicElasticsearchDeserializationSchema(
                deserializationSchema, producedTypeInformation);
    }

    private @Nullable DeserializationSchema<RowData> createDeserialization(
            DynamicTableSource.Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format) {
        if (format == null) {
            return null;
        }

        return format.createRuntimeDecoder(context, physicalDataType);
    }

    @Override
    public DynamicTableSource copy() {
        return new ElasticsearchDynamicSource(
                physicalDataType, producedDataType, decodingFormat, sourceConfig, tableIdentifier);
    }

    @Override
    public String asSummaryString() {
        return "Elasticsearch table source";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ElasticsearchDynamicSource that = (ElasticsearchDynamicSource) o;
        return Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(decodingFormat, that.decodingFormat)
                && Objects.equals(sourceConfig, that.sourceConfig)
                && Objects.equals(tableIdentifier, that.tableIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                producedDataType, physicalDataType, decodingFormat, sourceConfig, tableIdentifier);
    }
}
