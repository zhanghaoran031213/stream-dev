package com.stream.realtime.lululemon.API2;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.ConfigUtils;
import utils.EnvironmentSettingUtils;
import utils.KafkaUtils;
import utils.WaterMarkUtils;


import java.util.Date;

/**
 * @Author: ZHR
 * @Date: 2025/11/1 08:27
 * @Description:
 **/
public class DbusLogETLTask {

    private static final String KAFKA_BOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");

    private static final String KAFKA_LOG_TOPIC = "realtime_v3_logs";


    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);

        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/Flink_HDFS/flink-checkpoints");

        EnvironmentSettingUtils.defaultParameter(env);
        env.setStateBackend(new HashMapStateBackend());

        // 设置并行度
        env.setParallelism(1);


//        // 配置内存参数
//        Configuration config = new Configuration();
//        config.setString("taskmanager.memory.network.min", "64mb");
//        config.setString("taskmanager.memory.network.max", "128mb");
//        env.configure(config);


        DataStreamSource<String> kafkaLogDs = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(KAFKA_BOTSTRAP_SERVERS, KAFKA_LOG_TOPIC, new Date().toString(), OffsetsInitializer.earliest()),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5L),
                "_log_kafka_source_realtime_v3_logs"
        );

        // 2. 解析JSON日志
        SingleOutputStreamOperator<JSONObject> parsedLogStream = kafkaLogDs
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        try {
                            return JSON.parseObject(value);
                        } catch (Exception e) {
                            JSONObject errorLog = new JSONObject();
                            errorLog.put("parse_error", true);
                            return errorLog;
                        }
                    }
                })
                .filter(log -> !log.getBooleanValue("parse_error"));

//         3. 过滤空值和无效数据
        SingleOutputStreamOperator<JSONObject> filteredLogStream = parsedLogStream
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject log) throws Exception {
                        // 过滤掉关键字段为空的数据
                        return log != null
                                && log.containsKey("log_id")
                                && log.getString("log_id") != null
                                && !log.getString("log_id").isEmpty()
                                && log.containsKey("ts")
                                && log.getDouble("ts") != null
                                && log.getDouble("ts") > 0
                                && log.containsKey("opa")
                                && log.getString("opa") != null
                                && !log.getString("opa").isEmpty();
                    }
                })
                .name("filter_null_and_invalid_data");

//        filteredLogStream.print();



//        1. 历史天 + 当天 每个页面的总体访问量
//        ViewCountLogTest.calculatePageViewCount(filteredLogStream);


//        2. 历史天 + 当天 共计搜索词TOP10(每天的词云)
//        SearchKeywordMetrics.calculateDailyTop10Keywords(filteredLogStream); // 每日统计
//        SearchKeywordMetrics.calculateTotalTop10Keywords(filteredLogStream); // 总体统计
//        SearchKeywordMetrics.calculateRealTimeTop10Keywords(filteredLogStream); // 实时TOP10

        //        3. 历史天 + 当天 登陆区域的全国热力情况(每个地区的访问值)
//        HeatMapAnalysisUtils.analyzeHeatMap(kafkaLogDs);


        // 4. 历史天 + 当天 路径分析
//        SingleOutputStreamOperator<Tuple2<String, String>> userPaths = filteredLogStream
//                .filter(log -> log.containsKey("user_id") && log.getString("user_id") != null && !log.getString("user_id").isEmpty())
//                .map(log -> {
//                    String userId = log.getString("user_id");
//                    String page = log.getString("opa");
//                    Double ts = log.getDouble("ts");
//                    Long timestamp = (ts > 1e12) ? ts.longValue() : (long)(ts * 1000);
//
//                    if (timestamp < 1577808000000L) return Tuple2.of("INVALID", "INVALID");
//
//                    String dateStr = java.time.Instant.ofEpochMilli(timestamp)
//                            .atZone(java.time.ZoneId.systemDefault())
//                            .toLocalDate()
//                            .toString();
//
//                    return Tuple2.of(userId + "|" + dateStr, page + "|" + timestamp);
//                })
//                .returns(Types.TUPLE(Types.STRING, Types.STRING))
//                .filter(tuple -> !"INVALID".equals(tuple.f0));
//
//// 路径分析处理链（修复类型声明）
//        userPaths.keyBy(tuple -> tuple.f0)
//                .process(new UserPathAnalysisFunction())
//                .map(value -> {
//                    String date = value.f0.split("\\|")[0];
//                    return Tuple2.of(date + "|" + value.f1, 1L);
//                })
//                .returns(Types.TUPLE(Types.STRING, Types.LONG))  // 添加类型声明
//                .keyBy(tuple -> tuple.f0)
//                .sum(1)
//                .keyBy(value -> "global")
//                .process(new TopNPathFunction(10))
//                .print("===> TOP10 热门路径统计");


        // 5. 历史天 + 当天 用户设备的统计(ios & android (子类品牌(版本))) 下钻 (饼图 & 玫瑰图)
//        DeviceStatsMetrics.calculateDeviceStats(filteredLogStream);


        //6. 画像 每个用户 登陆的天数(分别是多少号)，在登陆期间 是否有购买行为，是否有搜索行为，是否有浏览行为，登陆时间是什么时间段 -> json -> es
//        UserProfileAnalysis.calculateUserProfile(filteredLogStream);


        env.execute();
    }

}
