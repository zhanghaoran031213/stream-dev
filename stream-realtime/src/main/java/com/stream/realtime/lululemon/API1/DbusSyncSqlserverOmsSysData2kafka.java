package com.stream.realtime.lululemon.API1;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.realtime.lululemon.API1.func.MapMergeJsonData;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.ConfigUtils;
import utils.EnvironmentSettingUtils;
import utils.KafkaUtils;

import java.util.Properties;

/**
 * @Author: ZHR
 * @Date: 2025/10/26 18:01
 * @Description:
 **/
public class DbusSyncSqlserverOmsSysData2kafka {

    private static final String KAFKA_BOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");

    private static final String OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC = "realtime_v3_order_info";
//private static final OutputTag<String> ERROR_PARSE_JSON_DATA_TAG = new OutputTag<String>("ERROR_PARSE_JSON_DATA_TAG"){};
    private static final String FLINK_UID_VERSION = "_v1";

    @SneakyThrows
    public static void main(String[] args) {

        boolean kafkaTopicDeFlag = KafkaUtils.kafkaTopicExists(KAFKA_BOTSTRAP_SERVERS, OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC);
        KafkaUtils.createKafkaTopic(KAFKA_BOTSTRAP_SERVERS,OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC,3,(short)1,kafkaTopicDeFlag);

        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        // 🔥 立即修复：使用本地文件系统 checkpoint
//        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");

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

        // 添加以下配置来处理 LSN 警告
        debeziumProperties.put("snapshot.isolation.mode", "snapshot");
        debeziumProperties.put("signal.data.collection", "dbo.oms_order_dtl");
        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("172.26.223.215")
                .port(1433)
                .username("sa")
                .password("Zhr123,./!")
                .database("realtime_v3")
                .tableList("dbo.oms_order_dtl")
//                .startupOptions(StartupOptions.latest())
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        DataStreamSource<String> dataStreamSource = env.addSource(sqlServerSource, "_transaction_log_source1");
//        dataStreamSource.print().setParallelism(1);
        SingleOutputStreamOperator<JSONObject> converStr2JsonDs = dataStreamSource.map(JSON::parseObject)
                .uid("converStr2JsonDs"+FLINK_UID_VERSION)
                .name("converStr2JsonDs");

        SingleOutputStreamOperator<JSONObject> resultJsonDs = converStr2JsonDs.map(new MapMergeJsonData())
                .uid("_MapMergeJsonData" + FLINK_UID_VERSION)
                .name("MapMergeJsonData");


        SingleOutputStreamOperator<String> jsonobj2str = resultJsonDs.map(JSONObject::toString)
                .uid("resultJsonDs" + FLINK_UID_VERSION)
                .name("resultJsonDs");

        jsonobj2str.print("Sink To Kafka Data: ->");
        jsonobj2str.sinkTo(
                KafkaUtils.buildKafkaSinkOrigin(KAFKA_BOTSTRAP_SERVERS,OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC)
        );





        env.execute("DbusSyncSqlserverOmsSysData2kafka");

    }

}
