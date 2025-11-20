package com.stream.realtime.lululemon.API2.func;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * 页面访问统计 - 支持输出到控制台和Doris（修复版）
 */
public class ViewCountLogTest {

    // 计算页面访问量并写入Doris（修复历史天+当天）
    public static void calculatePageViewCount(DataStream<JSONObject> source) {
        // 创建页面访问统计流 - 使用事件时间窗口
        SingleOutputStreamOperator<Tuple3<String, String, Long>> pageViewStream = source
                .filter(log -> {
                    // 过滤页面访问日志：根据log_type判断，而不是opa
                    String logType = log.getString("log_type");
                    boolean isPageView = logType != null && (
                            "search".equals(logType) ||
                                    "home".equals(logType) ||
                                    "product_list".equals(logType) ||
                                    "login".equals(logType) ||
                                    "product_detail".equals(logType) ||
                                    "payment".equals(logType)
                    );

                    if (isPageView) {
                        System.out.println("✅ 发现页面访问: " + logType + ", 时间戳: " + log.getDouble("ts"));
                    }
                    return isPageView;
                })
                .map(new MapFunction<JSONObject, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(JSONObject log) throws Exception {
                        // 从时间戳提取实际日期
                        Double timestamp = log.getDouble("ts");
                        String dateStr = extractDateFromTimestamp(timestamp);
                        String pageType = log.getString("log_type");

                        System.out.println("📊 处理页面访问 - 日期: " + dateStr + ", 页面: " + pageType + ", 原始时间戳: " + timestamp);
                        return Tuple3.of(dateStr, pageType, 1L);
                    }
                })
                .keyBy(tuple -> tuple.f0 + "|" + tuple.f1) // 按日期+页面类型分组
                .window(TumblingEventTimeWindows.of(Time.days(1))) // 按天窗口
                .sum(2) // 直接使用sum聚合
                .name("page_view_calculation");

        // 1. 输出到控制台
        pageViewStream
                .map(tuple -> String.format("📈 页面访问统计 - 日期: %s, 页面类型: %s, 访问量: %d",
                        tuple.f0, tuple.f1, tuple.f2))
                .print();

        // 2. 写入Doris - 使用新的sink方法
        pageViewStream
                .addSink(createPageViewSinkWithDate())
                .name("doris_page_view_sink")
                .setParallelism(1);
    }

    /**
     * 从时间戳提取日期字符串
     */
    private static String extractDateFromTimestamp(Double timestamp) {
        try {
            if (timestamp == null) {
                String currentDate = LocalDate.now().toString();
                System.out.println("⚠️ 时间戳为空，使用当前日期: " + currentDate);
                return currentDate;
            }

            long tsMillis;
            if (timestamp > 1e12) {
                // 已经是毫秒时间戳
                tsMillis = timestamp.longValue();
            } else {
                // 秒时间戳，转换为毫秒
                tsMillis = (long)(timestamp * 1000);
            }

            // 验证时间戳有效性 (时间戳应该大于 2020-01-01)
            if (tsMillis < 1577808000000L) {
                String currentDate = LocalDate.now().toString();
                System.out.println("⚠️ 时间戳过小: " + tsMillis + ", 使用当前日期: " + currentDate);
                return currentDate;
            }

            String dateStr = Instant.ofEpochMilli(tsMillis)
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate()
                    .toString();

            System.out.println("✅ 时间戳解析成功: " + tsMillis + " -> " + dateStr);
            return dateStr;

        } catch (Exception e) {
            String currentDate = LocalDate.now().toString();
            System.err.println("❌ 时间戳解析失败: " + timestamp + ", 使用当前日期: " + currentDate);
            return currentDate;
        }
    }

    /**
     * 创建支持日期的页面访问统计Sink
     */
    private static org.apache.flink.streaming.api.functions.sink.SinkFunction<Tuple3<String, String, Long>> createPageViewSinkWithDate() {
        String sql = "INSERT INTO page_view_stats (ds, page_name, pv) VALUES (?, ?, ?)";

        return org.apache.flink.connector.jdbc.JdbcSink.sink(
                sql,
                (statement, record) -> {
                    try {
                        // record.f0: 日期, record.f1: 页面类型, record.f2: 访问量
                        String dateStr = record.f0;
                        String pageName = record.f1;
                        Long pv = record.f2;

                        System.out.println("💾 写入Doris - 日期: " + dateStr + ", 页面: " + pageName + ", PV: " + pv);

                        // 验证日期格式
                        java.sql.Date sqlDate;
                        try {
                            sqlDate = java.sql.Date.valueOf(dateStr);
                        } catch (Exception e) {
                            System.err.println("❌ 日期格式错误: " + dateStr + ", 使用当前日期");
                            sqlDate = java.sql.Date.valueOf(LocalDate.now());
                        }

                        statement.setDate(1, sqlDate);
                        statement.setString(2, pageName);
                        statement.setLong(3, pv);

                    } catch (Exception e) {
                        System.err.println("❌ Doris写入失败: " + record + ", 错误: " + e.getMessage());
                        e.printStackTrace();
                    }
                },
                org.apache.flink.connector.jdbc.JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(5000)
                        .withMaxRetries(3)
                        .build(),
                new org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://172.26.223.215:9030/flink_analysis")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("")
                        .build()
        );
    }
}