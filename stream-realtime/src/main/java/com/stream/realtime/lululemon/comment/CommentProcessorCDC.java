package com.stream.realtime.lululemon.comment;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.realtime.lululemon.comment.func.SensitiveWordDetector;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.ConfigUtils;
import utils.EnvironmentSettingUtils;

import java.util.Properties;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * 评论处理器 - CDC风格修复版
 */
public class CommentProcessorCDC {

    private static final String FLINK_UID_VERSION = "_v1";
    // Kafka topic
    private static final String KAFKA_TOPIC = "realtime_v3_comment_cdc";

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        Properties debeziumProperties = new Properties();

        // Debezium 配置
        debeziumProperties.put("connect.timeout.ms", 10000);
        debeziumProperties.put("request.timeout.ms", 15000);
        debeziumProperties.put("heartbeat.interval.ms", 10000);
        debeziumProperties.put("snapshot.mode", "initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.put("snapshot.locking.mode", "none");
        debeziumProperties.put("snapshot.fetch.size", 200);
        debeziumProperties.put("snapshot.isolation.mode", "snapshot");
        debeziumProperties.put("signal.data.collection", "dbo.oms_order_user_comment");
        debeziumProperties.put("decimal.handling.mode", "double");
        debeziumProperties.put("binary.handling.mode", "base64");

        DataStreamSource<String> dataStreamSource = env.addSource(
                SqlServerSource.<String>builder()
                        .hostname("172.26.223.215")
                        .port(1433)
                        .username("sa")
                        .password("Zhr123,./!")
                        .database("realtime_v3")
                        .tableList("dbo.oms_order_user_comment")
                        .debeziumProperties(debeziumProperties)
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build(),
                "_comment_cdc_source"
        );

        // 解析JSON数据
        SingleOutputStreamOperator<JSONObject> converStr2JsonDs = dataStreamSource.map(JSON::parseObject)
                .uid("converStr2JsonDs" + FLINK_UID_VERSION)
                .name("converStr2JsonDs");

        // 处理评论数据（保留你原有处理逻辑），输出为 String（JSON文本）
        SingleOutputStreamOperator<String> resultDs = converStr2JsonDs
                .map(jsonNode -> processCommentData(jsonNode))
                .uid("processCommentData" + FLINK_UID_VERSION)
                .name("processCommentData");

        // *********************************
        //  ⭐ 写入 Kafka Sink ⭐
        // *********************************
        String kafkaBootstrap = ConfigUtils.getString("kafka.bootstrap.servers");
        if (kafkaBootstrap == null || kafkaBootstrap.trim().isEmpty()) {
            // fallback 默认值（如果没有在 ConfigUtils 配置）
            kafkaBootstrap = "172.26.223.200:9092";
        }

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(KAFKA_TOPIC)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 发送到 Kafka
        resultDs.sinkTo(kafkaSink)
                .uid("sinkToKafka" + FLINK_UID_VERSION)
                .name("sinkToKafka");

        // 控制台打印（开发调试用）
        resultDs.print("Comment Process Result: ->");

        System.out.println("启动 SQL Server 评论 CDC → Kafka 同步任务...");
        System.out.println("监控表: dbo.oms_order_user_comment");
        System.out.println("目标 Topic: " + KAFKA_TOPIC);
        System.out.println("kafka.bootstrap.servers = " + kafkaBootstrap);

        env.execute("CommentProcessorCDC");
    }

    /**
     * 处理评论数据 - 移除comment_id字段并构建最终输出 JSON
     */
    private static String processCommentData(JSONObject jsonNode) {
        try {
            String op = jsonNode.getString("op");
            JSONObject after = jsonNode.getJSONObject("after");

            if (after != null && ("c".equals(op) || "u".equals(op) || "r".equals(op))) {
                Long id = after.getLong("id");
                String userId = after.getString("user_id");
                // 注意：你原表字段名是 user_comment
                String commentContent = after.getString("user_comment");

                if (commentContent == null || commentContent.trim().isEmpty()) {
                    return "跳过空评论: ID=" + id;
                }

                if (id != null && userId != null) {
                    System.out.println("\n=== 开始处理评论 ID: " + id + " ===");

                    // 解析金额字段
                    Double totalAmount = parseAmountField(after);
                    if (totalAmount == null) {
                        totalAmount = extractAmountFromComment(commentContent);
                    }

                    // 获取评论时间
                    String commentTime = after.getString("ds");
                    Long timestamp = after.getLong("ts");

                    // 敏感词检测（调用你已有的检测器）
                    SensitiveWordDetector.SensitiveResult sensitiveResult = SensitiveWordDetector.detect(commentContent);

                    // 处理金额格式
                    int totalAmountInt = 0;
                    if (totalAmount != null) {
                        totalAmountInt = totalAmount.intValue();
                    } else {
                        totalAmountInt = extractAmountDirectly(commentContent);
                    }
                    System.out.println("💰 最终金额: " + totalAmountInt);

                    // 构建结果 - 移除comment_id字段
                    JSONObject result = new JSONObject();
                    result.put("order_id", after.getString("order_id"));
                    result.put("user_id", userId);
                    // 已移除 comment_id 字段
                    result.put("ds", commentTime);
                    result.put("ts", timestamp != null ? timestamp.toString() : String.valueOf(System.currentTimeMillis()));
                    result.put("is_insulting", sensitiveResult.isSensitive);
                    result.put("user_comment", commentContent);
                    result.put("db", "realtime_v3");
                    result.put("schema", "dbo");
                    result.put("table", "oms_order_user_comment");
                    result.put("sensitive_level", sensitiveResult.level);
                    result.put("is_blocked", sensitiveResult.isSensitive);
                    result.put("blacklist_duration_days", sensitiveResult.getBanDays());
                    result.put("triggered_keyword", sensitiveResult.triggeredKeyword != null ? sensitiveResult.triggeredKeyword : "");
                    result.put("keyword_source", "SENSITIVE_WORDS");
                    result.put("total_amount", totalAmountInt);

                    // 输出详细日志
                    if (sensitiveResult.isSensitive) {
                        System.out.println("🚨 敏感评论警报 - 用户: " + userId +
                                ", 级别: " + sensitiveResult.level +
                                ", 封禁: " + sensitiveResult.getBanDays() + "天" +
                                ", 金额: " + totalAmountInt +
                                ", 触发关键词: " + sensitiveResult.triggeredKeyword);
                        System.out.println("   订单: " + after.getString("order_id"));
                        System.out.println("   评论ID: " + id);
                        System.out.println("   检测到的所有词: " + sensitiveResult.foundWords);
                    } else {
                        System.out.println("✅ 正常评论 - 用户: " + userId +
                                ", 金额: " + totalAmountInt +
                                ", 订单: " + after.getString("order_id") +
                                ", 评论ID: " + id);
                    }
                    System.out.println("=== 结束处理评论 ID: " + id + " ===\n");

                    return result.toString();
                }
            }
        } catch (Exception e) {
            System.err.println("处理评论数据失败: " + e.getMessage());
            e.printStackTrace();
        }

        return "处理失败: " + jsonNode.toString();
    }

    /**
     * 解析金额字段
     */
    private static Double parseAmountField(JSONObject after) {
        if (after.containsKey("total_amount")) {
            Object amountNode = after.get("total_amount");

            if (amountNode != null) {
                try {
                    if (amountNode instanceof Number) {
                        return ((Number) amountNode).doubleValue();
                    } else if (amountNode instanceof String) {
                        String amountStr = ((String) amountNode).trim();
                        amountStr = amountStr.replaceAll("[^\\d.]", "");
                        if (!amountStr.isEmpty() && amountStr.matches("^\\d+(\\.\\d+)?$")) {
                            return Double.parseDouble(amountStr);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("金额解析异常: " + e.getMessage());
                }
            }
        }
        return null;
    }

    /**
     * 从评论中提取金额
     */
    private static Double extractAmountFromComment(String commentContent) {
        if (commentContent == null) return null;

        String[] patterns = {
                "(\\d{1,10}[.,]?\\d{0,2})\\s*(元|块|人民币|RMB|¥)",
                "价格.*?(\\d{1,10}[.,]?\\d{0,2})",
                "花了.*?(\\d{1,10}[.,]?\\d{0,2})",
                "买.*?(\\d{1,10}[.,]?\\d{0,2})",
                "\\b(\\d{3,5})\\b"
        };

        for (String patternStr : patterns) {
            try {
                Pattern pattern = Pattern.compile(patternStr);
                Matcher matcher = pattern.matcher(commentContent);

                if (matcher.find()) {
                    String amountStr = "";
                    if (matcher.groupCount() >= 1) {
                        amountStr = matcher.group(1);
                    } else {
                        amountStr = matcher.group();
                    }

                    amountStr = amountStr.replace(",", "").replace("，", "").replace(" ", "")
                            .replace("元", "").replace("块", "");

                    try {
                        double amount = Double.parseDouble(amountStr);
                        if (amount >= 100 && amount <= 100000) {
                            return amount;
                        }
                    } catch (NumberFormatException e) {
                        // 忽略格式错误
                    }
                }
            } catch (Exception e) {
                System.err.println("正则表达式匹配异常: " + e.getMessage());
            }
        }

        return null;
    }

    /**
     * 直接提取金额
     */
    private static int extractAmountDirectly(String commentContent) {
        if (commentContent == null) return 0;

        Pattern numberPattern = Pattern.compile("\\b(\\d{3,5})\\b");
        Matcher matcher = numberPattern.matcher(commentContent);

        while (matcher.find()) {
            String numberStr = matcher.group(1);
            try {
                int amount = Integer.parseInt(numberStr);
                if (amount >= 100 && amount <= 100000) {
                    return amount;
                }
            } catch (NumberFormatException e) {
                // 忽略
            }
        }

        return 0;
    }
}