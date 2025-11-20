package com.stream.realtime.lululemon.API2.func;

import com.alibaba.fastjson2.JSONObject;
import okhttp3.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @Author: ZHR
 * @Date: 2025/11/3 08:44
 * @Description: 用户画像分析 - 统计用户登录天数、行为特征等
 **/
public class UserProfileAnalysis {

    /**
     * 计算用户画像分析 - 使用 HTTP Sink 写入 ES
     */
    public static void calculateUserProfile(DataStream<JSONObject> filteredLogStream) {
        // 用户行为分析
        SingleOutputStreamOperator<String> userProfileStream = filteredLogStream
                .filter(log -> log.containsKey("user_id") &&
                        log.getString("user_id") != null &&
                        !log.getString("user_id").isEmpty())
                .keyBy(log -> log.getString("user_id"))
                .flatMap(new UserProfileAnalysisFunction())
                .name("user_profile_analysis");

        // 打印格式化输出到控制台
        SingleOutputStreamOperator<String> formattedOutput = userProfileStream
                .filter(data -> !data.startsWith("{"))
                .name("filter_formatted_output");

        formattedOutput.print("👤 用户画像分析");

        // 过滤出 JSON 格式的数据（用于ES写入）
        SingleOutputStreamOperator<String> esDataStream = userProfileStream
                .filter(data -> data.startsWith("{"))  // 过滤出JSON数据
                .name("filter_es_data");

        // 添加 HTTP ES Sink
        esDataStream
                .addSink(new HttpElasticsearchSink("http://localhost:9200"))
                .name("http_elasticsearch_sink")
                .setParallelism(1);
    }

    /**
     * HTTP Elasticsearch Sink - 使用 OkHttp 直接写入 ES
     */
    public static class HttpElasticsearchSink extends RichSinkFunction<String> {

        private transient OkHttpClient client;
        private final String esBaseUrl;
        private int successCount = 0;
        private int errorCount = 0;

        public HttpElasticsearchSink(String esBaseUrl) {
            this.esBaseUrl = esBaseUrl;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.client = new OkHttpClient.Builder()
                    .connectTimeout(30, TimeUnit.SECONDS)
                    .writeTimeout(30, TimeUnit.SECONDS)
                    .readTimeout(30, TimeUnit.SECONDS)
                    .retryOnConnectionFailure(true)
                    .build();

            System.out.println("✅ HTTP Elasticsearch Sink 初始化完成，ES地址: " + esBaseUrl);
        }

        @Override
        public void invoke(String jsonData, Context context) throws Exception {
            if (jsonData == null || jsonData.trim().isEmpty()) {
                return;
            }

            try {
                // 解析 JSON 获取用户ID作为文档ID
                JSONObject jsonObj = JSONObject.parseObject(jsonData);
                String userId = jsonObj.getString("user_id");

                if (userId == null || userId.isEmpty()) {
                    System.err.println("❌ 用户ID为空，跳过写入: " + jsonData.substring(0, Math.min(100, jsonData.length())));
                    errorCount++;
                    return;
                }

                // 构建 ES 请求 URL
                String url = esBaseUrl + "/user_behavior_profile/_doc/" + userId;

                // 创建请求体
                RequestBody body = RequestBody.create(
                        jsonData,
                        MediaType.parse("application/json; charset=utf-8")
                );

                // 构建请求
                Request request = new Request.Builder()
                        .url(url)
                        .header("Content-Type", "application/json")
                        .post(body)
                        .build();

                // 执行请求
                try (Response response = client.newCall(request).execute()) {
                    if (response.isSuccessful()) {
                        successCount++;
                        if (successCount % 10 == 0) {
                            System.out.println("✅ ES写入成功 [" + successCount + "]: " + userId);
                        }
                    } else {
                        errorCount++;
                        String errorBody = response.body() != null ? response.body().string() : "无响应体";
                        System.err.println("❌ ES写入失败 [" + errorCount + "]: " +
                                response.code() + " - " + response.message() + " - " + errorBody);
                    }
                }

            } catch (Exception e) {
                errorCount++;
                System.err.println("❌ ES写入异常 [" + errorCount + "]: " + e.getMessage());
                // 不抛出异常，避免作业失败
            }
        }

        @Override
        public void close() throws Exception {
            if (client != null) {
                client.dispatcher().executorService().shutdown();
                client.connectionPool().evictAll();
            }
            System.out.println("🔚 HTTP Elasticsearch Sink 关闭，成功: " + successCount + ", 失败: " + errorCount);
            super.close();
        }
    }

    /**
     * 用户画像分析函数
     */
    private static class UserProfileAnalysisFunction extends RichFlatMapFunction<JSONObject, String> {

        private transient ValueState<UserProfile> profileState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<UserProfile> descriptor =
                    new ValueStateDescriptor<>("user-profile", UserProfile.class);
            profileState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(JSONObject log, Collector<String> out) throws Exception {
            String userId = log.getString("user_id");
            Double timestamp = log.getDouble("ts");
            String opa = log.getString("opa");
            String logType = log.getString("log_type");

            // 获取或创建用户画像
            UserProfile profile = profileState.value();
            if (profile == null) {
                profile = new UserProfile(userId);
            }

            // 更新时间戳
            long eventTime = extractEpochMillis(timestamp);
            profile.updateLastActiveTime(eventTime);

            // 提取日期
            LocalDate eventDate = extractDateFromTimestamp(timestamp);
            profile.addLoginDate(eventDate);

            // 分析行为类型
            analyzeUserBehavior(profile, opa, logType, log);

            // 更新时间段分布
            profile.addTimeSegment(eventTime);

            // 保存状态
            profileState.update(profile);

            // 定期输出用户画像（每5条记录输出一次）
            if (profile.getEventCount() % 5 == 0) {
                // 输出格式化字符串用于控制台显示
                out.collect(profile.toFormattedString());

                // 输出JSON格式用于ES写入
                JSONObject esJson = profile.toESJSON();
                out.collect(esJson.toJSONString());
            }
        }
    }

    /**
     * 用户画像数据结构
     */
    public static class UserProfile {
        private String userId;
        private Set<LocalDate> loginDates = new HashSet<>();
        private Set<String> behaviorTypes = new HashSet<>();
        private Map<String, Integer> behaviorCounts = new HashMap<>();
        private Map<String, Integer> timeSegments = new HashMap<>();
        private long firstActiveTime = Long.MAX_VALUE;
        private long lastActiveTime = 0;
        private int eventCount = 0;

        public UserProfile(String userId) {
            this.userId = userId;
        }

        public void updateLastActiveTime(long timestamp) {
            this.lastActiveTime = Math.max(this.lastActiveTime, timestamp);
            this.firstActiveTime = Math.min(this.firstActiveTime, timestamp);
            this.eventCount++;
        }

        public void addLoginDate(LocalDate date) {
            loginDates.add(date);
        }

        public void addBehavior(String behavior) {
            behaviorTypes.add(behavior);
            behaviorCounts.put(behavior, behaviorCounts.getOrDefault(behavior, 0) + 1);
        }

        public void addTimeSegment(long timestamp) {
            LocalDateTime dateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
            int hour = dateTime.getHour();
            String segment = getTimeSegment(hour);
            timeSegments.put(segment, timeSegments.getOrDefault(segment, 0) + 1);
        }

        private String getTimeSegment(int hour) {
            if (hour >= 6 && hour < 12) return "morning";
            else if (hour >= 12 && hour < 14) return "noon";
            else if (hour >= 14 && hour < 18) return "afternoon";
            else if (hour >= 18 && hour < 22) return "evening";
            else return "night";
        }

        /**
         * 转换为ES专用的JSON格式
         */
        public JSONObject toESJSON() {
            JSONObject json = new JSONObject();
            json.put("user_id", userId);
            json.put("login_days_count", loginDates.size());

            // 排序日期
            List<String> sortedDates = new ArrayList<>();
            for (LocalDate date : loginDates) {
                sortedDates.add(date.toString());
            }
            Collections.sort(sortedDates);
            json.put("login_dates", sortedDates);

            // 行为标志
            json.put("has_purchase", behaviorTypes.contains("purchase"));
            json.put("has_search", behaviorTypes.contains("search"));
            json.put("has_browse", behaviorTypes.contains("browse"));
            json.put("has_pageview", behaviorTypes.contains("pageview"));

            // 行为计数
            json.put("purchase_count", behaviorCounts.getOrDefault("purchase", 0));
            json.put("search_count", behaviorCounts.getOrDefault("search", 0));
            json.put("browse_count", behaviorCounts.getOrDefault("browse", 0));

            // 时间段分析 - 找到最频繁的时间段
            String mostFrequentSegment = timeSegments.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElse("afternoon");
            json.put("login_time_period", mostFrequentSegment);

            // 时间字段
            json.put("last_login_time", lastActiveTime);
            json.put("update_time", System.currentTimeMillis());
            json.put("last_active_date", sortedDates.isEmpty() ? LocalDate.now().toString() : sortedDates.get(sortedDates.size() - 1));

            // 生成行为标签
            List<String> behaviorTags = new ArrayList<>();
            if (behaviorTypes.contains("purchase")) behaviorTags.add("active_buyer");
            if (behaviorTypes.contains("search")) behaviorTags.add("frequent_searcher");
            if (behaviorTypes.contains("browse")) behaviorTags.add("active_browser");
            if (loginDates.size() >= 7) behaviorTags.add("loyal_user");
            else if (loginDates.size() >= 3) behaviorTags.add("regular_user");
            else behaviorTags.add("new_user");

            json.put("behavior_tags", behaviorTags);
            json.put("active_days", loginDates.size());
            json.put("total_events", eventCount);

            return json;
        }

        public String toFormattedString() {
            JSONObject json = toESJSON();
            StringBuilder sb = new StringBuilder();

            sb.append("\n🎯 用户画像分析: ").append(userId).append("\n");
            sb.append("├─ 登录天数: ").append(json.getInteger("login_days_count")).append("天\n");
            sb.append("├─ 总事件数: ").append(json.getInteger("total_events")).append("次\n");

            // 显示具体登录日期
            List<String> loginDates = json.getList("login_dates", String.class);
            sb.append("├─ 登录日期: ").append(loginDates.size()).append("天\n");
            if (!loginDates.isEmpty()) {
                int maxDisplayDates = Math.min(3, loginDates.size());
                for (int i = 0; i < maxDisplayDates; i++) {
                    String prefix = (i == 0) ? "│  ├─ " : "│  │  ";
                    sb.append(prefix).append(loginDates.get(i));
                    if (i == maxDisplayDates - 1 && loginDates.size() > maxDisplayDates) {
                        sb.append(" ... 等").append(loginDates.size()).append("天");
                    }
                    sb.append("\n");
                }
                if (loginDates.size() > maxDisplayDates) {
                    sb.append("│  └─ 等").append(loginDates.size()).append("个登录日\n");
                } else {
                    sb.append("│  └─ 共").append(loginDates.size()).append("个登录日\n");
                }
            }

            // 行为分析
            sb.append("├─ 行为特征:\n");
            sb.append("│  ├─ 购买行为: ").append(json.getBoolean("has_purchase") ? "✅" : "❌").append("\n");
            sb.append("│  ├─ 搜索行为: ").append(json.getBoolean("has_search") ? "✅" : "❌").append("\n");
            sb.append("│  ├─ 浏览行为: ").append(json.getBoolean("has_browse") ? "✅" : "❌").append("\n");
            sb.append("│  └─ 页面访问: ").append(json.getBoolean("has_pageview") ? "✅" : "❌").append("\n");

            // 时间段分析
            String timePeriod = json.getString("login_time_period");
            sb.append("└─ 最活跃时段: ").append(getSegmentName(timePeriod)).append(" ").append(getSegmentEmoji(timePeriod)).append("\n");

            return sb.toString();
        }

        private String getSegmentEmoji(String segment) {
            switch (segment) {
                case "morning": return "🌅";
                case "noon": return "☀️";
                case "afternoon": return "🌤️";
                case "evening": return "🌆";
                case "night": return "🌙";
                default: return "⏰";
            }
        }

        private String getSegmentName(String segment) {
            switch (segment) {
                case "morning": return "早晨(6-12点)";
                case "noon": return "中午(12-14点)";
                case "afternoon": return "下午(14-18点)";
                case "evening": return "晚上(18-22点)";
                case "night": return "深夜(22-6点)";
                default: return segment;
            }
        }

        public int getEventCount() {
            return eventCount;
        }
    }

    /**
     * 分析用户行为
     */
    private static void analyzeUserBehavior(UserProfile profile, String opa, String logType, JSONObject log) {
        // 根据 opa 和 log_type 判断行为类型
        if ("search".equals(logType) || (log.containsKey("keywords") && log.getJSONArray("keywords") != null)) {
            profile.addBehavior("search");
        }

        if (log.containsKey("order_id") && log.getString("order_id") != null) {
            profile.addBehavior("purchase");
        }

        if ("pageinfo".equals(opa) || "pageview".equals(opa)) {
            profile.addBehavior("pageview");
            profile.addBehavior("browse");
        }

        if ("product".equals(opa) && log.containsKey("product_id")) {
            profile.addBehavior("product_view");
            profile.addBehavior("browse");
        }

        // 其他行为类型可以根据实际业务需求添加
        if ("click".equals(opa)) {
            profile.addBehavior("click");
        }

        if ("cart".equals(opa)) {
            profile.addBehavior("cart");
        }
    }

    /**
     * 从时间戳提取日期
     */
    private static LocalDate extractDateFromTimestamp(Double timestamp) {
        try {
            long epochMillis;
            if (timestamp > 1e12) {
                epochMillis = timestamp.longValue();
            } else {
                epochMillis = (long)(timestamp * 1000);
            }
            Instant instant = Instant.ofEpochMilli(epochMillis);
            return instant.atZone(ZoneId.systemDefault()).toLocalDate();
        } catch (Exception e) {
            return LocalDate.now();
        }
    }

    /**
     * 提取时间戳为毫秒
     */
    private static long extractEpochMillis(Double timestamp) {
        if (timestamp > 1e12) {
            return timestamp.longValue();
        } else {
            return (long)(timestamp * 1000);
        }
    }
}