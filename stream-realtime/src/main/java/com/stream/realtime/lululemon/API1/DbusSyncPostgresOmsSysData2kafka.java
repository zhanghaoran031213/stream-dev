package com.stream.realtime.lululemon.API1;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.realtime.lululemon.API1.func.MapMergeJsonData;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.EnvironmentSettingUtils;

import java.util.Properties;

/**
 * @Author: ZHR
 * @Date: 2025/10/26 21:27
 * @Description: PostgreSQL CDC 同步到 Kafka
 **/
public class DbusSyncPostgresOmsSysData2kafka {

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
        debeziumProperties.put("snapshot.fetch.size", 200);

        // 添加以下配置来处理 LSN 警告
        debeziumProperties.put("snapshot.isolation.mode", "snapshot");
        debeziumProperties.put("signal.data.collection", "dbo.oms_order_dtl");
        DebeziumSourceFunction<String> postgresSource = PostgreSQLSource.<String>builder()
                .hostname("172.26.223.215")
                .port(5432)
                .database("spider_db")
                .schemaList("public")
                .tableList("public.spider_lululemon_jd_product_dtl", "public.spider_lululemon_jd_product_info")
                .username("postgres")
                .password("Zhr123,./")
                .decodingPluginName("pgoutput")
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(postgresSource, "_transaction_log_source1");
//        dataStreamSource.print().setParallelism(1);
        SingleOutputStreamOperator<JSONObject> converStr2JsonDs = dataStreamSource.map(JSON::parseObject)
                .uid("converStr2JsonDs")
                .name("converStr2JsonDs");

        converStr2JsonDs.map(new MapMergeJsonData()).print();


//        SingleOutputStreamOperator<JSONObject> fixJsonDs = dataStreamSource.process(new ProcessFixJsonData(ERROR_PARSE_JSON_DATA_TAG))
//                .uid("fixJsonAndconverStr2JsonDs")
//                .name("fixJsonAndconverStr2JsonDs");
//
//
//        fixJsonDs.map(new MapMergeJsonData()).print();



        env.execute("DbusSyncPostgresOmsSysData2kafka");

    }
}