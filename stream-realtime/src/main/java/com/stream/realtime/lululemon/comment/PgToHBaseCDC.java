package com.stream.realtime.lululemon.comment;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// HBase 依赖
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import utils.EnvironmentSettingUtils;

import java.util.Properties;

public class PgToHBaseCDC {

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);

        Properties debeziumProperties = new Properties();

        // 在创建 source 前添加超时配置
        debeziumProperties.put("connect.timeout.ms", 10000);
        debeziumProperties.put("request.timeout.ms", 15000);
        debeziumProperties.put("heartbeat.interval.ms", 10000);

        debeziumProperties.put("snapshot.mode", "initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.put("snapshot.locking.mode", "none");
//        debeziumProperties.put("snapshot.mode", "schema_only");
        debeziumProperties.put("snapshot.fetch.size", 200);

        // ========== PostgreSQL CDC ==========
        DebeziumSourceFunction<String> pgSource = PostgreSQLSource.<String>builder()
                .hostname("172.26.223.215")
                .port(5432)
                .database("spider_db")
                .schemaList("public")
                .tableList("public.user_info_base_Constellation")
                .username("postgres")
                .password("Zhr123,./")
                .decodingPluginName("pgoutput")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> stream = env.addSource(pgSource);

        stream
                .map(JSON::parseObject)
                .addSink(new HBaseSink())  // ★ 自定义 HBase Sink
                .name("HBaseSink");

        env.execute("PgToHBaseCDC");
    }

    // ===================================================================
    //                          正确的 HBase Sink
    // ===================================================================
    public static class HBaseSink implements org.apache.flink.streaming.api.functions.sink.SinkFunction<JSONObject> {

        private static Connection connection;
        private static Table table;
        private static final String TABLE_NAME = "realtime_v3:dim_user_info_base_Constellation";

        @Override
        public void invoke(JSONObject json, Context context) throws Exception {

            if (json == null) return;

            JSONObject after = json.getJSONObject("after");
            if (after == null || after.isEmpty()) return;

            String userId = after.getString("user_id");
            if (userId == null) return;

            if (connection == null) {

                Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
                conf.set("zookeeper.znode.parent", "/hbase");

                connection = ConnectionFactory.createConnection(conf);
                table = connection.getTable(TableName.valueOf(TABLE_NAME));
            }

            Put put = new Put(Bytes.toBytes(userId));

            // 写入字段 => 列族 info
            for (String key : after.keySet()) {
                String value = after.getString(key);
                if (value != null) {
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(key), Bytes.toBytes(value));
                }
            }

            table.put(put);
        }
    }
}
