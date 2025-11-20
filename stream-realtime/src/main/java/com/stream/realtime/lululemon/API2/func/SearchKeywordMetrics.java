package com.stream.realtime.lululemon.API2.func;

import com.alibaba.fastjson2.JSONObject;
import com.stream.realtime.lululemon.API2.utils.DorisSinkUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: ZHR
 * @Date: 2025/11/1 11:00
 * @Description: 搜索词统计 - 简化版本，直接写入Doris
 **/
public class SearchKeywordMetrics {

    /**
     * 计算每天搜索词并写入Doris（简化版本）
     */
    public static void calculateDailyTop10Keywords(DataStream<JSONObject> source) {
        // 创建每日搜索词统计流
        SingleOutputStreamOperator<Tuple3<String, String, Long>> dailyKeywordStream = source
                .filter(log -> "search".equals(log.getString("log_type")))
                .filter(log -> log.containsKey("keywords") && log.getJSONArray("keywords") != null)
                .flatMap(new FlatMapFunction<JSONObject, Tuple3<String, String, Long>>() {
                    @Override
                    public void flatMap(JSONObject log, Collector<Tuple3<String, String, Long>> collector) throws Exception {
                        Long ts = log.getLong("ts");
                        if (ts == null) return;

                        // 处理时间戳
                        long millis = ts < 1000000000000L ? ts * 1000 : ts;
                        String date = Instant.ofEpochMilli(millis)
                                .atZone(ZoneId.of("Asia/Shanghai"))
                                .toLocalDate()
                                .toString();

                        // 提取搜索关键词
                        List<String> keywords = log.getJSONArray("keywords").toList(String.class);
                        for (String keyword : keywords) {
                            if (keyword != null && !keyword.trim().isEmpty()) {
                                collector.collect(Tuple3.of(date, keyword.trim(), 1L));
                            }
                        }
                    }
                })
                .keyBy(t -> t.f0 + "_" + t.f1) // 按日期+关键词分组
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5))) // 5分钟窗口
                .sum(2) // 直接使用sum聚合
                .name("daily_keyword_calculation");

        // 1. 输出到控制台
        dailyKeywordStream
                .map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) throws Exception {
                        return String.format("📅 日期: %s | 🔍 关键词: %s | 🔥 搜索量: %d",
                                value.f0, value.f1, value.f2);
                    }
                })
                .print("每日搜索词统计");

        // 2. 写入Doris - 使用标准INSERT
        dailyKeywordStream
                .addSink(DorisSinkUtils.createDailyKeywordSink())
                .name("doris_daily_keyword_sink")
                .setParallelism(1);
    }

    /**
     * 计算总体搜索词并写入Doris（简化版本）
     */
    public static void calculateTotalTop10Keywords(DataStream<JSONObject> source) {
        // 创建总体搜索词统计流
        SingleOutputStreamOperator<Tuple2<String, Long>> totalKeywordStream = source
                .filter(log -> "search".equals(log.getString("log_type")))
                .filter(log -> log.containsKey("keywords") && log.getJSONArray("keywords") != null)
                .flatMap(new FlatMapFunction<JSONObject, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(JSONObject log, Collector<Tuple2<String, Long>> collector) throws Exception {
                        List<String> keywords = log.getJSONArray("keywords").toList(String.class);
                        for (String keyword : keywords) {
                            if (keyword != null && !keyword.trim().isEmpty()) {
                                collector.collect(Tuple2.of(keyword.trim(), 1L));
                            }
                        }
                    }
                })
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5))) // 5分钟窗口
                .sum(1) // 直接使用sum聚合
                .name("total_keyword_calculation");

        // 1. 输出到控制台
        totalKeywordStream
                .map(new MapFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public String map(Tuple2<String, Long> value) throws Exception {
                        return String.format("🔍 关键词: %s | 🔥 总搜索量: %d", value.f0, value.f1);
                    }
                })
                .print("关键词搜索量统计");

        // 2. 写入Doris - 使用标准INSERT
        totalKeywordStream
                .addSink(DorisSinkUtils.createTotalKeywordSink())
                .name("doris_total_keyword_sink")
                .setParallelism(1);
    }

    /**
     * 计算实时TOP10搜索词并写入Doris（简化版本）
     */
    public static void calculateRealTimeTop10Keywords(DataStream<JSONObject> source) {
        SingleOutputStreamOperator<Tuple2<String, Long>> realTimeKeywordStream = source
                .filter(log -> "search".equals(log.getString("log_type")))
                .filter(log -> log.containsKey("keywords") && log.getJSONArray("keywords") != null)
                .flatMap(new FlatMapFunction<JSONObject, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(JSONObject log, Collector<Tuple2<String, Long>> collector) throws Exception {
                        List<String> keywords = log.getJSONArray("keywords").toList(String.class);
                        for (String keyword : keywords) {
                            if (keyword != null && !keyword.trim().isEmpty()) {
                                collector.collect(Tuple2.of(keyword.trim(), 1L));
                            }
                        }
                    }
                })
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2))) // 2分钟窗口
                .sum(1) // 直接使用sum聚合
                .name("realtime_keyword_calculation");

        // 1. 输出TOP10到控制台
        realTimeKeywordStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .process(new SimpleTopNFunction(10))
                .print("实时TOP10搜索词");

        // 2. 写入Doris - 使用标准INSERT
        realTimeKeywordStream
                .addSink(DorisSinkUtils.createTotalKeywordSink())
                .name("doris_realtime_keyword_sink")
                .setParallelism(1);
    }

    // SimpleTopNFunction 保持不变
    public static class SimpleTopNFunction extends org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction<
            Tuple2<String, Long>, String, TimeWindow> {

        private final int topN;

        public SimpleTopNFunction(int topN) {
            this.topN = topN;
        }

        @Override
        public void process(Context context, Iterable<Tuple2<String, Long>> elements,
                            Collector<String> out) throws Exception {

            List<Tuple2<String, Long>> list = new ArrayList<>();
            for (Tuple2<String, Long> element : elements) {
                list.add(element);
            }

            // 按搜索量降序排序
            list.sort((a, b) -> Long.compare(b.f1, a.f1));

            // 输出TOP N
            out.collect("🏆 实时搜索词TOP" + topN + ":");
            int count = Math.min(topN, list.size());
            for (int i = 0; i < count; i++) {
                Tuple2<String, Long> item = list.get(i);
                out.collect(String.format("%d. 🔍 %s | 🔥 %d次", i + 1, item.f0, item.f1));
            }
            out.collect("=======================");
        }
    }
}