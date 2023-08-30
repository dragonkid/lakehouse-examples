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
import java.util.Map;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

public class PaimonStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(PaimonStream.class);

    private static final Map<String, RowKind> ROW_KIND_MAP =
            ImmutableMap.of("I", RowKind.INSERT, "D", RowKind.DELETE);

    public static void main(String[] args) throws Exception {
        LOGGER.info("PaimonStream started with args: {}", String.join(",", args));
        ParameterTool parameters = ParameterTool.fromArgs(args);

        // init env
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
        }).returns(Types.ROW_NAMED(new String[] {"block_number", "hash", "timestamp", "type"},
                Types.INT, Types.STRING, Types.INSTANT, Types.STRING));

        // init catalog
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        TableConfig tableConfig = tableEnv.getConfig();
        tableConfig.set("table.exec.sink.upsert-materialize", "NONE");


        tableEnv.executeSql("CREATE CATALOG paimon WITH (\n" + //
                "    'type' = 'paimon',\n" + //
                "    'warehouse' = 's3://iris/',\n" + //
                "    's3.endpoint' = 'http://127.0.0.1:9000',\n" + //
                "    's3.access-key' = 'minio',\n" + //
                "    's3.secret-key' = 'minio123'\n" + //
                ");");
        tableEnv.useCatalog("paimon");

        // init tableq
        Schema schema = Schema.newBuilder().column("block_number", DataTypes.INT())
                .column("hash", DataTypes.STRING()).column("timestamp", DataTypes.TIMESTAMP_LTZ())
                .column("type", DataTypes.STRING()).build();
        Table table = tableEnv.fromChangelogStream(stream, schema, ChangelogMode.all());
        tableEnv.createTemporaryView("sink", table);


        tableEnv.executeSql("drop table if exists test.test");
        tableEnv.executeSql(
                "create table test.test (block_number int, hash string, `timestamp` timestamp, type string, primary key (block_number) not enforced) with ('merge-engine' = 'deduplicate')");

        // insert into paimon table from your data stream table
        tableEnv.executeSql("INSERT INTO test.test SELECT * FROM sink");
    }
}
