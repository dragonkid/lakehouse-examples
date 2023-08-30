package com.arecadata.clickstream;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class StreamRead {
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(Integer.parseInt(parameters.get("checkpoint", "5000")));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE CATALOG paimon WITH (\n" + //
                "    'type' = 'paimon',\n" + //
                // " 'metastore' = 'hive',\n" + //
                // " 'uri' = 'thrift://localhost:9083',\n" + //
                "    'warehouse' = 's3://iris/',\n" + //
                "    's3.endpoint' = 'http://127.0.0.1:9000',\n" + //
                "    's3.access-key' = 'minio',\n" + //
                "    's3.secret-key' = 'minio123'\n" + //
                ");");
        tableEnv.useCatalog("paimon");

        Table table = tableEnv.sqlQuery("select * from test.test");
        DataStream<Row> dataStream = tableEnv.toChangelogStream(table);

        dataStream.executeAndCollect().forEachRemaining(System.out::println);
    }
}
