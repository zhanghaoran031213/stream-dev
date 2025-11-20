// HeatMapAnalysisUtils.java
package com.stream.realtime.lululemon.API2.func;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 热力图分析工具类 - 完全保留原有逻辑效果
 */
public class HeatMapAnalysisUtils {

    /**
     * 执行热力图分析 - 完全保留原有处理逻辑
     */
    public static void executeHeatMapAnalysis(DataStream<String> kafkaLogDs) {
        // 1. 解析JSON字符串为JsonObject - 完全复制原有逻辑
        SingleOutputStreamOperator<JsonObject> jsonLogStream = kafkaLogDs
                .map(new MapFunction<String, JsonObject>() {
                    private transient JsonParser jsonParser;

                    @Override
                    public JsonObject map(String value) throws Exception {
                        if (jsonParser == null) {
                            jsonParser = new JsonParser();
                        }
                        try {
                            return jsonParser.parse(value).getAsJsonObject();
                        } catch (Exception e) {
                            JsonObject errorObj = new JsonObject();
                            errorObj.addProperty("parse_error", true);
                            return errorObj;
                        }
                    }
                })
                .filter(json -> !json.has("parse_error"))
                .name("parse_json_log");

        // 2. 添加地理位置信息
        SingleOutputStreamOperator<JsonObject> withLocation = jsonLogStream
                .map(new IpLocationEnrichment())
                .name("ip-location-enrichment");

        // 3. 使用详细位置信息获取省份和城市 - 完全复制原有逻辑
        DataStream<Tuple3<String, String, Long>> detailedRegionCounts = withLocation
                .filter(json -> json.has("gis") && json.get("gis").isJsonObject())
                .filter(json -> {
                    JsonObject gis = json.getAsJsonObject("gis");
                    return gis.has("ip") && !gis.get("ip").isJsonNull();
                })
                .map(json -> {
                    String ip = json.getAsJsonObject("gis").get("ip").getAsString();
                    String[] detailedLocation = IPLocationUtils.getDetailedLocation(ip);
                    String province = detailedLocation[0];
                    String city = detailedLocation[1];

                    // 过滤掉未知位置和内网 - 完全复制原有逻辑
                    if (!"未知".equals(province) && !"内网".equals(province) &&
                            !"未知".equals(city) && !"内网".equals(city)) {
                        return Tuple3.of(province, city, 1L);
                    }
                    return Tuple3.of("未知", "未知", 0L);
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .filter(tuple -> tuple.f2 > 0) // 过滤掉未知位置
                .name("detailed-region-counts");

        // 4. 按省份和城市分组统计 - 完全复制原有逻辑
        DataStream<Tuple3<String, String, Long>> provinceCityCounts = detailedRegionCounts
                .keyBy(tuple -> tuple.f0 + "|" + tuple.f1) // 按省份+城市作为key
                .sum(2)
                .name("province-city-counts");

        // 5. 收集统计信息并生成热力图报告 - 完全复制原有逻辑
        provinceCityCounts
                .keyBy(value -> "global")
                .process(new OriginalHeatMapReportFunction())
                .print("🌍 全国省份城市热力图>");
    }

    /**
     * 原始的热力图报告函数 - 完全复制原有逻辑
     */
    public static class OriginalHeatMapReportFunction extends KeyedProcessFunction<String, Tuple3<String, String, Long>, String> {

        private transient Map<String, Map<String, Long>> provinceCityMap;
        private transient long totalVisits;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            provinceCityMap = new HashMap<>();
            totalVisits = 0;
            System.out.println("✅ 热力图报告函数初始化完成 - 保留原有逻辑");
        }

        @Override
        public void processElement(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            String province = value.f0;
            String city = value.f1;
            Long count = value.f2;

            // 更新省份-城市映射 - 完全复制原有逻辑
            provinceCityMap.computeIfAbsent(province, k -> new HashMap<>())
                    .merge(city, count, Long::sum);

            totalVisits += count;

            // 生成热力图报告 - 完全复制原有逻辑
            String report = IPLocationUtils.generateHeatMapReport(provinceCityMap, totalVisits);
            out.collect(report);
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("✅ 热力图分析完成，总访问量: " + totalVisits + " - 保留原有逻辑");
        }
    }

    /**
     * 提供更简洁的调用方法
     */
    public static void analyzeHeatMap(DataStream<String> kafkaLogStream) {
        executeHeatMapAnalysis(kafkaLogStream);
    }
}