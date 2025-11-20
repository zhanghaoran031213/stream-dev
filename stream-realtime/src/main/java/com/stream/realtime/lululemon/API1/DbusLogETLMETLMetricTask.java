package com.stream.realtime.lululemon.API1;

import lombok.SneakyThrows;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.ConfigUtils;
import utils.EnvironmentSettingUtils;
import utils.KafkaUtils;
import utils.WaterMarkUtils;

import java.util.Date;

/**
 * @Author: ZHR
 * @Date: 2025/10/31 21:03
 * @Description:
 **/
public class DbusLogETLMETLMetricTask {

    private static final String KAFKA_BOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");

    private static final String KAFKA_LOG_TOPIC = "realtime_v3_logs";

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String> kafkaLogDs = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(KAFKA_BOTSTRAP_SERVERS, KAFKA_LOG_TOPIC, new Date().toString(), OffsetsInitializer.earliest()),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5L),
                "_log_kafka_source_realtime_v3_logs"
        );


        kafkaLogDs.print();

        env.execute();
    }

}
