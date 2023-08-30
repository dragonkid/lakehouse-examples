/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.arecadata.clickstream;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arecadata.clickstream.sink.FlinkSink;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class IcebergStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergStream.class);

    private static final Map<String, RowKind> ROW_KIND_MAP =
            ImmutableMap.of("I", RowKind.INSERT, "D", RowKind.DELETE);

    public static void main(String[] args) throws Exception {
        LOGGER.info("ClickStream started with args: {}", String.join(",", args));
        ParameterTool parameters = ParameterTool.fromArgs(args);
        // init catalog
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("type", "iceberg");
        catalogProperties.put("catalog-type", "hive");
        catalogProperties.put("uri", parameters.get("uri", "thrift://localhost:9083"));
        catalogProperties.put("io-impl",
                parameters.get("io-impl", "org.apache.iceberg.aws.s3.S3FileIO"));
        catalogProperties.put("warehouse", parameters.get("warehouse", "s3a://iris/"));
        catalogProperties.put("s3.endpoint",
                parameters.get("s3-endpoint", "http://127.0.0.1:9000"));
        Configuration hadoopConf = new Configuration();
        CatalogLoader catalogLoader = CatalogLoader.hive("flink", hadoopConf, catalogProperties);
        Catalog catalog = catalogLoader.loadCatalog();

        // init table
        Schema schema =
                new Schema(Types.NestedField.required(1, "block_number", Types.IntegerType.get()),
                        Types.NestedField.required(2, "hash", Types.StringType.get()),
                        Types.NestedField.required(3, "timestamp", Types.TimestampType.withZone()),
                        Types.NestedField.required(4, "type", Types.StringType.get()));

        String databaseName = parameters.get("database", "test");
        String tableName = parameters.get("table", "test");
        TableIdentifier outputTable = TableIdentifier.of(databaseName, tableName);
        if (!catalog.tableExists(outputTable)) {
            catalog.createTable(outputTable, schema, PartitionSpec.unpartitioned(), ImmutableMap.of(
                    // "write.upsert.enabled", "true",
                    "format-version", "2"));
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(Integer.parseInt(parameters.get("checkpoint", "5000")));

        // use socketTextStream source
        DataStream<String> dataStream =
                env.addSource(new SocketTextStreamFunction("localhost", 9999, "\n", -1));

        DataStream<Row> stream = dataStream.filter(s -> {
            String[] fields = s.split(",");
            return fields.length == 3;
        }).map(s -> {
            String[] fields = s.split(",");
            RowKind rowKind = ROW_KIND_MAP.get(fields[0]);
            return Row.ofKind(rowKind, Integer.parseInt(fields[1]), fields[2],
                    Instant.ofEpochSecond(Instant.now().getEpochSecond()), fields[0]);
        });

        // use faker source
        // FakerSource source = new FakerSource();
        // source.setEventInterval(Float.parseFloat(parameters.get("event_interval", "5000")));
        // DataStream<Row> stream =
        // env.addSource(source).returns(TypeInformation.of(Map.class)).map(s -> {
        // RowKind rowKind = ROW_KIND_MAP.get(s.get("type"));
        // // Row row = new Row(4);
        // // row.setField(0, s.get("character"));
        // // row.setField(1, s.get("location"));
        // // row.setField(2, s.get("event_time"));
        // // row.setField(3, s.get("type"));
        // return Row.ofKind(rowKind, s.get("block_number"), s.get("hash"),
        // s.get("timestamp"), s.get("type"));
        // });

        // Configure row-based append
        FlinkSink.forRow(stream, FlinkSchemaUtil.toSchema(schema))
                .tableLoader(TableLoader.fromCatalog(catalogLoader, outputTable))
                .equalityFieldColumns(ImmutableList.of("block_number"))
                // .upsert(true)
                .append();
        // Execute the flink app
        env.execute();
    }
}
