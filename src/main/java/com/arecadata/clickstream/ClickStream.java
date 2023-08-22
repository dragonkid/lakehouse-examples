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

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
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

public class ClickStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickStream.class);

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
                new Schema(Types.NestedField.required(1, "character", Types.StringType.get()),
                        Types.NestedField.required(2, "location", Types.StringType.get()),
                        Types.NestedField.required(3, "event_time", Types.TimestampType.withZone()),
                        Types.NestedField.required(4, "type", Types.StringType.get()));

        String databaseName = parameters.get("database", "test");
        String tableName = parameters.get("table", "clickstream");
        TableIdentifier outputTable = TableIdentifier.of(databaseName, tableName);
        if (!catalog.tableExists(outputTable)) {
            catalog.createTable(outputTable, schema, PartitionSpec.unpartitioned());
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(Integer.parseInt(parameters.get("checkpoint", "10000")));

        FakerSource source = new FakerSource();
        source.setEventInterval(Float.parseFloat(parameters.get("event_interval", "5000")));
        DataStream<Row> stream =
                env.addSource(source).returns(TypeInformation.of(Map.class)).map(s -> {
                    Row row = new Row(4);
                    row.setField(0, s.get("character"));
                    row.setField(1, s.get("location"));
                    row.setField(2, s.get("event_time"));
                    row.setField(3, s.get("type"));
                    return row;
                });
        // Configure row-based append
        FlinkSink.forRow(stream, FlinkSchemaUtil.toSchema(schema))
                .tableLoader(TableLoader.fromCatalog(catalogLoader, outputTable)).append();
        // Execute the flink app
        env.execute();
    }
}
