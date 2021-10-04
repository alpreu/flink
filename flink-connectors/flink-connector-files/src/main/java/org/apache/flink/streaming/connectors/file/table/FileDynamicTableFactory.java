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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.BulkReaderFormatFactory;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.BulkWriterFormatFactory;
import org.apache.flink.table.factories.DecodingFormatFactory;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.EncodingFormatFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import static java.time.ZoneId.SHORT_IDS;

/** Factory for creating configured instances of {@link FileDynamicSink}. */
@Internal
public class FileDynamicTableFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "file";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        validate(helper);
        return new FileDynamicSink(
                context,
                discoverDecodingFormat(context, BulkReaderFormatFactory.class),
                discoverDecodingFormat(context, DeserializationFormatFactory.class),
                discoverEncodingFormat(context, BulkWriterFormatFactory.class),
                discoverEncodingFormat(context, SerializationFormatFactory.class));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FileConnectorOptions.PATH);
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FileConnectorOptions.PARTITION_DEFAULT_NAME);
        options.add(FileConnectorOptions.SINK_ROLLING_POLICY_FILE_SIZE);
        options.add(FileConnectorOptions.SINK_ROLLING_POLICY_ROLLOVER_INTERVAL);
        options.add(FileConnectorOptions.SINK_ROLLING_POLICY_CHECK_INTERVAL);
        options.add(FileConnectorOptions.SINK_SHUFFLE_BY_PARTITION);
        options.add(FileConnectorOptions.PARTITION_TIME_EXTRACTOR_KIND);
        options.add(FileConnectorOptions.PARTITION_TIME_EXTRACTOR_CLASS);
        options.add(FileConnectorOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN);
        options.add(FileConnectorOptions.SINK_PARTITION_COMMIT_TRIGGER);
        options.add(FileConnectorOptions.SINK_PARTITION_COMMIT_DELAY);
        options.add(FileConnectorOptions.SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE);
        options.add(FileConnectorOptions.SINK_PARTITION_COMMIT_POLICY_KIND);
        options.add(FileConnectorOptions.SINK_PARTITION_COMMIT_POLICY_CLASS);
        options.add(FileConnectorOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME);
        options.add(FileConnectorOptions.SINK_PARALLELISM);
        return options;
    }

    private void validate(FactoryUtil.TableFactoryHelper helper) {
        // Except format options, some formats like parquet and orc can not list all supported
        // options.
        helper.validateExcept(helper.getOptions().get(FactoryUtil.FORMAT) + ".");

        // validate time zone of watermark
        String watermarkTimeZone =
                helper.getOptions()
                        .get(FileConnectorOptions.SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE);
        if (watermarkTimeZone.startsWith("UTC+")
                || watermarkTimeZone.startsWith("UTC-")
                || SHORT_IDS.containsKey(watermarkTimeZone)) {
            throw new ValidationException(
                    String.format(
                            "The supported watermark time zone is either a full name such as 'America/Los_Angeles',"
                                    + " or a custom time zone id such as 'GMT-08:00', but configured time zone is '%s'.",
                            watermarkTimeZone));
        }
    }

    private <I, F extends DecodingFormatFactory<I>> DecodingFormat<I> discoverDecodingFormat(
            Context context, Class<F> formatFactoryClass) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        if (formatFactoryExists(context, formatFactoryClass)) {
            return helper.discoverDecodingFormat(formatFactoryClass, FactoryUtil.FORMAT);
        } else {
            return null;
        }
    }

    private <I, F extends EncodingFormatFactory<I>> EncodingFormat<I> discoverEncodingFormat(
            Context context, Class<F> formatFactoryClass) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        if (formatFactoryExists(context, formatFactoryClass)) {
            return helper.discoverEncodingFormat(formatFactoryClass, FactoryUtil.FORMAT);
        } else {
            return null;
        }
    }

    /**
     * Returns true if the format factory can be found using the given factory base class and
     * identifier.
     */
    private boolean formatFactoryExists(Context context, Class<?> factoryClass) {
        Configuration options = Configuration.fromMap(context.getCatalogTable().getOptions());
        String identifier = options.get(FactoryUtil.FORMAT);
        if (identifier == null) {
            throw new ValidationException(
                    String.format(
                            "Table options do not contain an option key '%s' for discovering a format.",
                            FactoryUtil.FORMAT.key()));
        }

        final List<Factory> factories = new LinkedList<>();
        ServiceLoader.load(Factory.class, context.getClassLoader())
                .iterator()
                .forEachRemaining(factories::add);

        final List<Factory> foundFactories =
                factories.stream()
                        .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                        .collect(Collectors.toList());

        final List<Factory> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.factoryIdentifier().equals(identifier))
                        .collect(Collectors.toList());

        return !matchingFactories.isEmpty();
    }
}
