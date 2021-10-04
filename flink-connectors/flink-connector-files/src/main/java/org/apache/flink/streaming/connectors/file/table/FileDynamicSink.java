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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.PartitionComputer;
import org.apache.flink.table.connector.RowDataPartitionComputer;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.streaming.connectors.file.table.FileConnectorOptions.SINK_ROLLING_POLICY_CHECK_INTERVAL;
import static org.apache.flink.streaming.connectors.file.table.FileConnectorOptions.SINK_ROLLING_POLICY_FILE_SIZE;
import static org.apache.flink.streaming.connectors.file.table.FileConnectorOptions.SINK_ROLLING_POLICY_ROLLOVER_INTERVAL;

/** A file system {@link DynamicTableSink}. */
@Internal
public class FileDynamicSink extends AbstractFileSystemTable
        implements DynamicTableSink, SupportsPartitioning {

    // For compaction reading
    @Nullable private final DecodingFormat<BulkFormat<RowData, FileSourceSplit>> bulkReaderFormat;
    @Nullable private final DecodingFormat<DeserializationSchema<RowData>> deserializationFormat;

    // For Writing
    @Nullable private final EncodingFormat<BulkWriter.Factory<RowData>> bulkWriterFormat;
    @Nullable private final EncodingFormat<SerializationSchema<RowData>> serializationFormat;

    private boolean dynamicGrouping = false;
    private LinkedHashMap<String, String> staticPartitions = new LinkedHashMap<>();

    @Nullable private Integer configuredParallelism;

    FileDynamicSink(
            DynamicTableFactory.Context context,
            @Nullable DecodingFormat<BulkFormat<RowData, FileSourceSplit>> bulkReaderFormat,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> deserializationFormat,
            @Nullable EncodingFormat<BulkWriter.Factory<RowData>> bulkWriterFormat,
            @Nullable EncodingFormat<SerializationSchema<RowData>> serializationFormat) {
        super(context);
        this.bulkReaderFormat = bulkReaderFormat;
        this.deserializationFormat = deserializationFormat;
        if (Stream.of(bulkWriterFormat, serializationFormat).allMatch(Objects::isNull)) {
            Configuration options = Configuration.fromMap(context.getCatalogTable().getOptions());
            String identifier = options.get(FactoryUtil.FORMAT);
            throw new ValidationException(
                    String.format(
                            "Could not find any format factory for identifier '%s' in the classpath.",
                            identifier));
        }
        this.bulkWriterFormat = bulkWriterFormat;
        this.serializationFormat = serializationFormat;
        this.configuredParallelism = tableOptions.get(FileConnectorOptions.SINK_PARALLELISM);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        checkConfiguredParallelismAllowed(requestedMode);
        if (bulkWriterFormat != null) {
            return bulkWriterFormat.getChangelogMode();
        } else if (serializationFormat != null) {
            return serializationFormat.getChangelogMode();
        } else {
            throw new TableException("Can not find format factory.");
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context sinkContext) {
        RowDataPartitionComputer computer = partitionComputer();

        Object writer = createWriter(sinkContext);
        boolean isEncoder = writer instanceof Encoder;

        FileDynamicSink.TableBucketAssigner assigner =
                new FileDynamicSink.TableBucketAssigner(computer);
        FileDynamicSink.TableRollingPolicy rollingPolicy =
                new FileDynamicSink.TableRollingPolicy(
                        !isEncoder,
                        tableOptions.get(SINK_ROLLING_POLICY_FILE_SIZE).getBytes(),
                        tableOptions.get(SINK_ROLLING_POLICY_ROLLOVER_INTERVAL).toMillis());

        String randomPrefix = "part-" + UUID.randomUUID().toString();
        OutputFileConfig fileNamingConfig =
                OutputFileConfig.builder().withPartPrefix(randomPrefix).build();

        long bucketCheckInterval = tableOptions.get(SINK_ROLLING_POLICY_CHECK_INTERVAL).toMillis();

        FileSink<RowData> sink;
        if (isEncoder) {
            //noinspection unchecked
            sink =
                    FileSink.forRowFormat(
                                    path,
                                    new FileDynamicSink.ProjectionEncoder(
                                            (Encoder<RowData>) writer, computer))
                            .withBucketAssigner(assigner)
                            .withOutputFileConfig(fileNamingConfig)
                            .withRollingPolicy(rollingPolicy)
                            .withBucketCheckInterval(bucketCheckInterval)
                            .build();
        } else {
            //noinspection unchecked
            sink =
                    FileSink.forBulkFormat(
                                    path,
                                    new FileDynamicSink.ProjectionBulkFactory(
                                            (BulkWriter.Factory<RowData>) writer, computer))
                            .withBucketAssigner(assigner)
                            .withOutputFileConfig(fileNamingConfig)
                            .withRollingPolicy(rollingPolicy)
                            .withBucketCheckInterval(bucketCheckInterval)
                            .build();
        }

        return SinkProvider.of(sink, configuredParallelism);
    }

    @Override
    public DynamicTableSink copy() {
        FileDynamicSink sink =
                new FileDynamicSink(
                        context,
                        bulkReaderFormat,
                        deserializationFormat,
                        bulkWriterFormat,
                        serializationFormat);
        sink.dynamicGrouping = dynamicGrouping;
        sink.staticPartitions = staticPartitions;
        return sink;
    }

    @Override
    public String asSummaryString() {
        return "Filesystem";
    }

    @Override
    public boolean requiresPartitionGrouping(boolean supportsGrouping) {
        this.dynamicGrouping = supportsGrouping;
        return dynamicGrouping;
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        this.staticPartitions = toPartialLinkedPartSpec(partition);
    }

    private LinkedHashMap<String, String> toPartialLinkedPartSpec(Map<String, String> part) {
        LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
        for (String partitionKey : partitionKeys) {
            if (part.containsKey(partitionKey)) {
                partSpec.put(partitionKey, part.get(partitionKey));
            }
        }
        return partSpec;
    }

    private RowDataPartitionComputer partitionComputer() {
        return new RowDataPartitionComputer(
                defaultPartName,
                schema.getFieldNames(),
                schema.getFieldDataTypes(),
                partitionKeys.toArray(new String[0]));
    }

    private Object createWriter(Context sinkContext) {
        if (bulkWriterFormat != null) {
            return bulkWriterFormat.createRuntimeEncoder(sinkContext, getFormatDataType());
        } else if (serializationFormat != null) {
            return new SerializationSchemaAdapter(
                    serializationFormat.createRuntimeEncoder(sinkContext, getFormatDataType()));
        } else {
            throw new TableException("Can not find format factory.");
        }
    }

    private void checkConfiguredParallelismAllowed(ChangelogMode requestChangelogMode) {
        final Integer parallelism = this.configuredParallelism;
        if (parallelism == null) {
            return;
        }
        if (!requestChangelogMode.containsOnly(RowKind.INSERT)) {
            throw new ValidationException(
                    String.format(
                            "Currently, filesystem sink doesn't support setting parallelism (%d) by '%s' "
                                    + "when the input stream is not INSERT only. The row kinds of input stream are [%s]",
                            parallelism,
                            FileConnectorOptions.SINK_PARALLELISM.key(),
                            requestChangelogMode.getContainedKinds().stream()
                                    .map(RowKind::shortString)
                                    .collect(Collectors.joining(","))));
        }
    }

    /** Table bucket assigner, wrap {@link PartitionComputer}. */
    public static class TableBucketAssigner implements BucketAssigner<RowData, String> {

        private final PartitionComputer<RowData> computer;

        public TableBucketAssigner(PartitionComputer<RowData> computer) {
            this.computer = computer;
        }

        @Override
        public String getBucketId(RowData element, Context context) {
            try {
                return PartitionPathUtils.generatePartitionPath(
                        computer.generatePartValues(element));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }

    /** Table {@link RollingPolicy}, it extends {@link CheckpointRollingPolicy} for bulk writers. */
    public static class TableRollingPolicy extends CheckpointRollingPolicy<RowData, String> {

        private final boolean rollOnCheckpoint;
        private final long rollingFileSize;
        private final long rollingTimeInterval;

        public TableRollingPolicy(
                boolean rollOnCheckpoint, long rollingFileSize, long rollingTimeInterval) {
            this.rollOnCheckpoint = rollOnCheckpoint;
            Preconditions.checkArgument(rollingFileSize > 0L);
            Preconditions.checkArgument(rollingTimeInterval > 0L);
            this.rollingFileSize = rollingFileSize;
            this.rollingTimeInterval = rollingTimeInterval;
        }

        @Override
        public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState) {
            try {
                return rollOnCheckpoint || partFileState.getSize() > rollingFileSize;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, RowData element)
                throws IOException {
            return partFileState.getSize() > rollingFileSize;
        }

        @Override
        public boolean shouldRollOnProcessingTime(
                PartFileInfo<String> partFileState, long currentTime) {
            return currentTime - partFileState.getCreationTime() >= rollingTimeInterval;
        }
    }

    private static class ProjectionEncoder implements Encoder<RowData> {

        private final Encoder<RowData> encoder;
        private final RowDataPartitionComputer computer;

        private ProjectionEncoder(Encoder<RowData> encoder, RowDataPartitionComputer computer) {
            this.encoder = encoder;
            this.computer = computer;
        }

        @Override
        public void encode(RowData element, OutputStream stream) throws IOException {
            encoder.encode(computer.projectColumnsToWrite(element), stream);
        }
    }

    /** Project row to non-partition fields. */
    public static class ProjectionBulkFactory implements BulkWriter.Factory<RowData> {

        private final BulkWriter.Factory<RowData> factory;
        private final RowDataPartitionComputer computer;

        public ProjectionBulkFactory(
                BulkWriter.Factory<RowData> factory, RowDataPartitionComputer computer) {
            this.factory = factory;
            this.computer = computer;
        }

        @Override
        public BulkWriter<RowData> create(FSDataOutputStream out) throws IOException {
            BulkWriter<RowData> writer = factory.create(out);
            return new BulkWriter<RowData>() {

                @Override
                public void addElement(RowData element) throws IOException {
                    writer.addElement(computer.projectColumnsToWrite(element));
                }

                @Override
                public void flush() throws IOException {
                    writer.flush();
                }

                @Override
                public void finish() throws IOException {
                    writer.finish();
                }
            };
        }
    }
}
