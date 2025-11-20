package com.stream.realtime.lululemon.API3;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import utils.ConfigUtils;
import utils.EnvironmentSettingUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author: ZHR
 * @Description: 用户标签生成器 - 基于全量历史数据的消费等级和用户偏好分析（分区版）
 **/
public class UserLabelGeneratorFixed {

    private static final String KAFKA_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String TOPIC_LOG = "realtime_v3_logs";
    private static final String TOPIC_COMMENT = "realtime_v3_comment_cdc";
    private static final String HBASE_TABLE = "realtime_v3:dim_user_info_base_Constellation";
    private static final String CF = "info";

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettingUtils.defaultParameter(env);

        env.setStateBackend(new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                new org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage()
        );

        // 1. 从Kafka读取日志数据
        DataStreamSource<String> logStream = env.fromSource(
                KafkaSource.<String>builder()
                        .setBootstrapServers(KAFKA_SERVERS)
                        .setTopics(TOPIC_LOG)
                        .setGroupId("user_label_complete")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .build(),
                WatermarkStrategy.noWatermarks(),
                "Kafka-Log-Source"
        );

        // 2. 从Kafka读取评论数据
        DataStreamSource<String> commentStream = env.fromSource(
                KafkaSource.<String>builder()
                        .setBootstrapServers(KAFKA_SERVERS)
                        .setTopics(TOPIC_COMMENT)
                        .setGroupId("user_label_complete")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .build(),
                WatermarkStrategy.noWatermarks(),
                "Kafka-Comment-Source"
        );

        // 3. 解析日志数据
        SingleOutputStreamOperator<JSONObject> parsedLogStream = logStream
                .map(new LogParser())
                .name("Log-Parser");

        // 4. 关联HBase用户信息
        SingleOutputStreamOperator<JSONObject> logWithUserInfoStream = parsedLogStream
                .keyBy(log -> getPartitionKey(log.getString("user_id"), log.getString("ds")))
                .map(new HBaseSyncLookupFunction())
                .name("HBase-Enrich");

        // 5. 解析评论数据
        SingleOutputStreamOperator<JSONObject> parsedCommentStream = commentStream
                .map(new CommentParser())
                .name("Comment-Parser");

        // 6. 合并两个流进行标签聚合（使用KeyedProcessFunction）
        DataStream<JSONObject> finalUserLabelStream = logWithUserInfoStream
                .union(parsedCommentStream)
                .keyBy(data -> getPartitionKey(data.getString("user_id"), data.getString("ds")))
                .process(new UserLabelAggregationFunction())
                .name("User-Label-Aggregation");

        // 7. 过滤null值并输出
        SingleOutputStreamOperator<JSONObject> filteredLabelStream = finalUserLabelStream
                .filter(label -> label != null)
                .name("Filter-Null-Labels");

        filteredLabelStream.print("Final User Label: ");
        env.execute("User Label Generator - Partition Version");
    }

    /**
     * 生成分区键：user_id + ds
     */
    private static String getPartitionKey(String userId, String ds) {
        return userId + "|" + ds;
    }

    /**
     * 日志数据解析器
     */
    public static class LogParser implements org.apache.flink.api.common.functions.MapFunction<String, JSONObject> {
        private static final Set<String> SYSTEM_FIELDS = new HashSet<>(Arrays.asList(
                "pageinfo", "duration", "adinfo", "user_id", "log_id", "ts", "product_id",
                "order_id", "log_type", "opa", "device", "network", "gis", "network_type"
        ));

        @Override
        public JSONObject map(String value) throws Exception {
            JSONObject log = JSON.parseObject(value);
            JSONObject result = new JSONObject();

            String userId = log.getString("user_id");
            result.put("user_id", userId);
            result.put("log_id", log.getString("log_id"));

            // 时间戳处理
            Long timestamp = standardizeTimestamp(log.getLong("ts"));
            result.put("ts", timestamp);
            String ds = formatTimestampToDate(timestamp);
            result.put("ds", ds);
            result.put("pt", getPartitionKey(userId, ds));

            // 产品ID处理
            String productId = processProductId(log.getString("product_id"));
            result.put("product_id", productId);

            result.put("order_id", log.getString("order_id"));
            result.put("log_type", log.getString("log_type"));
            result.put("opa", log.getString("opa"));

            // 设备信息
            if (log.containsKey("device")) {
                result.put("device", log.getJSONObject("device"));
            }

            // 网络信息
            if (log.containsKey("network")) {
                JSONObject network = log.getJSONObject("network");
                result.put("network", network);
                if (network != null) {
                    result.put("network_type", network.getString("network_type"));
                }
            }

            // GIS信息
            if (log.containsKey("gis")) {
                result.put("gis", log.getJSONObject("gis"));
            }

            // 关键词提取
            JSONArray keywords = extractKeywords(log);
            if (!keywords.isEmpty()) {
                result.put("keywords", keywords);
            }

            // 标记数据来源
            result.put("data_type", "log");

            return result;
        }

        private Long standardizeTimestamp(Long timestamp) {
            if (timestamp == null) return System.currentTimeMillis();
            if (timestamp >= 1e12 && timestamp < 1e13) return timestamp; // 13位毫秒
            if (timestamp >= 1e9 && timestamp < 1e10) return timestamp * 1000; // 10位秒级
            return System.currentTimeMillis();
        }

        private String processProductId(String productId) {
            if (productId == null) return null;
            productId = productId.replace("\n", "").trim();
            if (productId.contains("item.jd.com")) {
                return extractProductIdFromUrl(productId);
            }
            return productId;
        }

        private JSONArray extractKeywords(JSONObject log) {
            JSONArray keywordsArray = new JSONArray();
            Set<String> allKeywords = new HashSet<>();

            // 从keywords字段提取
            if (log.containsKey("keywords")) {
                extractFromKeywordsField(log.get("keywords"), allKeywords);
            }

            // 从评论提取
            if (log.containsKey("user_comment")) {
                extractFromComment(log.getString("user_comment"), allKeywords);
            }

            // 过滤有效关键词
            for (String keyword : allKeywords) {
                if (isValidKeyword(keyword)) {
                    keywordsArray.add(keyword.trim());
                }
            }

            return keywordsArray;
        }

        private void extractFromKeywordsField(Object keywordsObj, Set<String> allKeywords) {
            if (keywordsObj instanceof JSONArray) {
                JSONArray array = (JSONArray) keywordsObj;
                for (int i = 0; i < array.size(); i++) {
                    String keyword = array.getString(i);
                    if (keyword != null && !keyword.trim().isEmpty()) {
                        allKeywords.add(keyword.trim());
                    }
                }
            } else if (keywordsObj instanceof String) {
                String[] keywords = ((String) keywordsObj).split("[,，;；\\s]+");
                for (String keyword : keywords) {
                    if (keyword != null && !keyword.trim().isEmpty()) {
                        allKeywords.add(keyword.trim());
                    }
                }
            }
        }

        private void extractFromComment(String comment, Set<String> allKeywords) {
            if (comment != null && !comment.trim().isEmpty()) {
                String[] words = comment.split("[\\s\\p{Punct}]+");
                for (String word : words) {
                    if (word.length() >= 2 && word.length() <= 10) {
                        allKeywords.add(word);
                    }
                }
            }
        }

        private boolean isValidKeyword(String keyword) {
            if (keyword == null || keyword.trim().isEmpty()) return false;
            String trimmed = keyword.trim().toLowerCase();

            if (SYSTEM_FIELDS.contains(trimmed)) return false;
            if (trimmed.length() < 1 || trimmed.length() > 20) return false;
            if (trimmed.matches("^\\d+$")) return false;
            if (trimmed.contains("http") || trimmed.contains("@")) return false;

            return true;
        }

        private String extractProductIdFromUrl(String url) {
            try {
                if (url.contains("/")) {
                    String[] parts = url.split("/");
                    String lastPart = parts[parts.length - 1];
                    if (lastPart.contains(".")) {
                        return lastPart.split("\\.")[0];
                    }
                    return lastPart;
                }
                return url;
            } catch (Exception e) {
                return url;
            }
        }

        private String formatTimestampToDate(Long timestamp) {
            try {
                return Instant.ofEpochMilli(timestamp)
                        .atZone(ZoneId.of("Asia/Shanghai"))
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            } catch (Exception e) {
                return LocalDate.now().toString();
            }
        }
    }

    /**
     * 评论数据解析器
     */
    public static class CommentParser implements org.apache.flink.api.common.functions.MapFunction<String, JSONObject> {
        @Override
        public JSONObject map(String value) throws Exception {
            JSONObject comment = JSON.parseObject(value);
            JSONObject result = new JSONObject();

            String userId = comment.getString("user_id");
            result.put("user_id", userId);
            result.put("order_id", comment.getString("order_id"));

            // 时间处理
            String ds = comment.getString("ds").substring(0, 10); // 取日期部分
            result.put("ds", ds);
            result.put("pt", getPartitionKey(userId, ds));

            Long timestamp = standardizeTimestamp(comment.getLong("ts"));
            result.put("ts", timestamp);

            result.put("is_insulting", comment.getBoolean("is_insulting"));
            result.put("user_comment", comment.getString("user_comment"));
            result.put("sensitive_level", comment.getString("sensitive_level"));
            result.put("is_blocked", comment.getBoolean("is_blocked"));
            result.put("triggered_keyword", comment.getString("triggered_keyword"));
            result.put("total_amount", comment.getDoubleValue("total_amount"));

            // 标记数据来源
            result.put("data_type", "comment");

            return result;
        }

        private Long standardizeTimestamp(Long timestamp) {
            if (timestamp == null) return System.currentTimeMillis();
            if (timestamp >= 1e12 && timestamp < 1e13) return timestamp;
            if (timestamp >= 1e9 && timestamp < 1e10) return timestamp * 1000;
            return System.currentTimeMillis();
        }
    }

    /**
     * HBase用户信息查询
     */
    public static class HBaseSyncLookupFunction extends org.apache.flink.api.common.functions.RichMapFunction<JSONObject, JSONObject> {
        private transient Connection connection;
        private transient Table table;

        @Override
        public void open(Configuration parameters) throws Exception {
            org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
            config.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(HBASE_TABLE));
        }

        @Override
        public JSONObject map(JSONObject log) throws Exception {
            String userId = log.getString("user_id");
            JSONObject userInfo = new JSONObject();

            try {
                Get get = new Get(Bytes.toBytes(userId));
                get.addFamily(Bytes.toBytes(CF));
                Result result = table.get(get);

                if (!result.isEmpty()) {
                    userInfo.put("username", getValue(result, "uname"));
                    userInfo.put("birthday", getValue(result, "birthday"));
                    userInfo.put("age", getValue(result, "age"));
                    userInfo.put("constellation", getValue(result, "constellation"));
                    userInfo.put("gender", convertGenderCode(getValue(result, "gender")));

                    String yearBest = getValue(result, "year_best");
                    userInfo.put("decade", calculateDecade(yearBest));
                    userInfo.put("address", getValue(result, "address"));
                    userInfo.put("phone_num", getValue(result, "phone_num"));
                    userInfo.put("year_best", yearBest);
                } else {
                    setDefaultUserInfo(userInfo, userId);
                }
            } catch (Exception e) {
                setDefaultUserInfo(userInfo, userId);
            }

            log.put("user_base_info_raw", userInfo);
            return log;
        }

        private String getValue(Result result, String column) {
            byte[] value = result.getValue(Bytes.toBytes(CF), Bytes.toBytes(column));
            return value == null ? null : Bytes.toString(value);
        }

        private String convertGenderCode(String genderCode) {
            if (genderCode == null) return "未知";
            switch (genderCode.trim()) {
                case "0": case "女": return "女";
                case "1": case "男": return "男";
                case "2": case "其他": return "其他";
                default: return "未知";
            }
        }

        private String calculateDecade(String yearBest) {
            if (yearBest == null || yearBest.isEmpty()) return "90后";
            return yearBest.length() >= 2 ? yearBest.substring(0, 2) + "后" : yearBest + "后";
        }

        private void setDefaultUserInfo(JSONObject userInfo, String userId) {
            userInfo.put("username", "用户_" + userId.substring(0, Math.min(8, userId.length())));
            userInfo.put("birthday", "1990-01-01");
            userInfo.put("age", "30");
            userInfo.put("constellation", "未知");
            userInfo.put("gender", "未知");
            userInfo.put("decade", "90后");
            userInfo.put("address", "未知");
            userInfo.put("phone_num", "未知");
            userInfo.put("year_best", "90");
        }

        @Override
        public void close() throws Exception {
            if (table != null) table.close();
            if (connection != null) connection.close();
        }
    }

    /**
     * 用户标签聚合函数 - 基于KeyedProcessFunction（修复版）
     */
    public static class UserLabelAggregationFunction extends KeyedProcessFunction<String, JSONObject, JSONObject> {

        // 状态定义 - 严格区分当天和历史
        private transient ValueState<JSONObject> userBaseInfoState;
        private transient ListState<String> todayLoginState;        // 当天登录时间戳
        private transient ListState<JSONObject> deviceState;        // 所有历史设备
        private transient ListState<String> todaySearchState;       // 当天搜索关键词
        private transient ListState<String> todayShoppingState;     // 当天浏览商品
        private transient ListState<JSONObject> sensitiveWordState; // 所有历史敏感词
        private transient ValueState<Double> totalSpendState;       // 全量历史消费金额
        private transient ListState<String> allTimeSearchState;     // 全量历史搜索关键词
        private transient ListState<String> allTimeShoppingState;   // 全量历史浏览商品

        // 新增：按日期分桶的状态，用于时间范围分析
        private transient MapState<String, List<String>> dailySearchState;    // 按日期存储搜索关键词
        private transient MapState<String, List<String>> dailyShoppingState;  // 按日期存储浏览商品

        private static final Set<String> FILTER_KEYWORDS = new HashSet<>(Arrays.asList(
                "null", "undefined", "", "opa", "log_type", "network_type", "product_id", "order_id"
        ));

        @Override
        public void open(Configuration parameters) throws Exception {
            userBaseInfoState = getRuntimeContext().getState(new ValueStateDescriptor<>("userBaseInfoState", JSONObject.class));
            todayLoginState = getRuntimeContext().getListState(new ListStateDescriptor<>("todayLoginState", String.class));
            deviceState = getRuntimeContext().getListState(new ListStateDescriptor<>("deviceState", JSONObject.class));
            todaySearchState = getRuntimeContext().getListState(new ListStateDescriptor<>("todaySearchState", String.class));
            todayShoppingState = getRuntimeContext().getListState(new ListStateDescriptor<>("todayShoppingState", String.class));
            sensitiveWordState = getRuntimeContext().getListState(new ListStateDescriptor<>("sensitiveWordState", JSONObject.class));
            totalSpendState = getRuntimeContext().getState(new ValueStateDescriptor<>("totalSpendState", Double.class));
            allTimeSearchState = getRuntimeContext().getListState(new ListStateDescriptor<>("allTimeSearchState", String.class));
            allTimeShoppingState = getRuntimeContext().getListState(new ListStateDescriptor<>("allTimeShoppingState", String.class));

            // 新增分桶状态初始化 - 使用明确的类型信息
            dailySearchState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("dailySearchState",
                            Types.STRING,
                            Types.LIST(Types.STRING))
            );
            dailyShoppingState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("dailyShoppingState",
                            Types.STRING,
                            Types.LIST(Types.STRING))
            );
        }

        @Override
        public void processElement(JSONObject data, Context ctx, Collector<JSONObject> out) throws Exception {
            String dataType = data.getString("data_type");

            if ("log".equals(dataType)) {
                processLogData(data, ctx, out);
            } else if ("comment".equals(dataType)) {
                processCommentData(data, ctx, out);
            }
        }

        private void processLogData(JSONObject log, Context ctx, Collector<JSONObject> out) throws Exception {
            String userId = log.getString("user_id");
            Long ts = log.getLong("ts");
            String logDate = log.getString("ds");
            String today = LocalDate.now().toString();

            // 保存用户基础信息
            if (log.containsKey("user_base_info_raw")) {
                userBaseInfoState.update(log.getJSONObject("user_base_info_raw"));
            }

            // 当天登录记录 - 严格去重
            if (today.equals(logDate)) {
                String loginTime = formatTimestamp(ts);
                // 检查是否已记录该时间点的登录（精确到秒级去重）
                boolean exists = false;
                for (String existingTime : todayLoginState.get()) {
                    if (existingTime.equals(loginTime)) {
                        exists = true;
                        break;
                    }
                }
                if (!exists) {
                    todayLoginState.add(loginTime);
                }
            }

            // 设备信息 - 全量历史记录
            if (log.containsKey("device")) {
                updateDeviceInfo(log.getJSONObject("device"));
            }

            // 关键词处理
            if (log.containsKey("keywords")) {
                processKeywords(log.getJSONArray("keywords"), logDate);
            }

            // 购物商品处理
            String productId = log.getString("product_id");
            if (isValidProductId(productId)) {
                productId = processProductId(productId);

                // 全量历史记录
                allTimeShoppingState.add(productId);

                // 按日期分桶记录
                addToDailyState(dailyShoppingState, logDate, productId);

                // 当天记录（严格限制为当天）
                if (today.equals(logDate)) {
                    todayShoppingState.add(productId);
                }
            }

            // 输出用户标签
            JSONObject userLabel = buildUserLabel(userId, logDate);
            if (userLabel != null) {
                out.collect(userLabel);
            }
        }

        private void processCommentData(JSONObject comment, Context ctx, Collector<JSONObject> out) throws Exception {
            String userId = comment.getString("user_id");
            String commentDate = comment.getString("ds");

            // 敏感词记录 - 全量历史记录
            if (!"CLEAN".equals(comment.getString("sensitive_level"))) {
                sensitiveWordState.add(createSensitiveWordRecord(comment));
            }

            // 更新消费金额 - 全量历史累计
            Double amount = comment.getDoubleValue("total_amount");
            if (amount != null && amount > 0) {
                Double currentTotal = totalSpendState.value();
                totalSpendState.update(currentTotal != null ? currentTotal + amount : amount);
            }

            // 输出用户标签
            JSONObject userLabel = buildUserLabel(userId, commentDate);
            if (userLabel != null) {
                out.collect(userLabel);
            }
        }

        private JSONObject buildUserLabel(String userId, String ds) throws Exception {
            JSONObject userBaseInfo = userBaseInfoState.value();
            if (userBaseInfo == null) {
                userBaseInfo = createDefaultUserInfo(userId);
                userBaseInfoState.update(userBaseInfo);
            }

            JSONObject label = new JSONObject();
            label.put("userid", userId);
            label.put("username", userBaseInfo.getString("username"));
            label.put("user_base_info", buildUserBaseInfo(userBaseInfo));

            // 当天行为 - 严格限制为当天数据
            label.put("login_time", buildTodayLoginTimes());
            label.put("device_info", buildDeviceInfo());
            label.put("today_behavior", buildTodayBehavior());

            // 全量历史分析 - 基于所有历史数据
            label.put("search_info", buildSearchInfo());
            label.put("category_info", buildCategoryInfo());
            label.put("shoping_gender", buildShoppingGender(userBaseInfo));
            label.put("sensitive_word", buildSensitiveWords());
            label.put("consumption_level", calculateConsumptionLevel());

            // 消费金额 - 全量历史累计
            Double totalSpend = totalSpendState.value();
            label.put("total_spend_amount", totalSpend != null ? totalSpend : 0.0);
            label.put("is_check_sensitive_comment", hasSensitiveWords() ? "1" : "0");

            // 时间信息和分区信息
            long currentTime = System.currentTimeMillis();
            label.put("ds", ds);
            label.put("ts", currentTime);
            label.put("pt", getPartitionKey(userId, ds));

            return label;
        }

        // ========== 数据处理方法 ==========

        private void processKeywords(JSONArray keywords, String logDate) throws Exception {
            if (keywords != null) {
                for (int i = 0; i < keywords.size(); i++) {
                    String keyword = keywords.getString(i);
                    if (isValidKeyword(keyword)) {
                        // 全量历史记录
                        allTimeSearchState.add(keyword);

                        // 按日期分桶记录
                        addToDailyState(dailySearchState, logDate, keyword);

                        // 当天记录（严格限制为当天）
                        if (LocalDate.now().toString().equals(logDate)) {
                            todaySearchState.add(keyword);
                        }
                    }
                }
            }
        }

        private void addToDailyState(MapState<String, List<String>> state, String date, String value) throws Exception {
            List<String> dailyList = state.get(date);
            if (dailyList == null) {
                dailyList = new ArrayList<>();
            }
            // 去重添加
            if (!dailyList.contains(value)) {
                dailyList.add(value);
            }
            state.put(date, dailyList);
        }

        private void updateDeviceInfo(JSONObject device) throws Exception {
            if (device != null && !device.isEmpty()) {
                String deviceKey = device.getString("brand") + "_" + device.getString("device");
                boolean exists = false;
                for (JSONObject existing : deviceState.get()) {
                    String existingKey = existing.getString("brand") + "_" + existing.getString("device");
                    if (deviceKey.equals(existingKey)) {
                        exists = true;
                        break;
                    }
                }
                if (!exists) {
                    deviceState.add(device);
                }
            }
        }

        // ========== 标签构建方法 ==========

        private JSONObject buildTodayBehavior() throws Exception {
            JSONObject behavior = new JSONObject();

            // 当天浏览商品数 - 严格去重计数
            behavior.put("viewed_products_count", getUniqueCount(todayShoppingState));

            // 当天搜索次数 - 严格去重计数
            behavior.put("search_count", getUniqueCount(todaySearchState));

            // 当天登录次数 - 基于时间戳精确去重
            behavior.put("login_count", getTodayLoginCount());

            return behavior;
        }

        private JSONObject buildSearchInfo() throws Exception {
            JSONObject searchInfo = new JSONObject();

            // 最近关键词 - 基于所有历史数据
            JSONArray keywords = getUniqueKeywords(allTimeSearchState);
            searchInfo.put("recent_keywords", keywords);
            searchInfo.put("search_count", keywords.size());

            return searchInfo;
        }

        private JSONObject buildCategoryInfo() throws Exception {
            JSONObject categoryInfo = new JSONObject();

            // 偏好品类 - 基于所有历史搜索数据计算
            JSONArray preferredCategories = calculatePreferredCategories();
            categoryInfo.put("preferred_categories", preferredCategories);

            // 最近浏览商品 - 基于所有历史数据
            JSONArray recentProducts = getUniqueProducts(allTimeShoppingState);
            categoryInfo.put("recent_viewed_products", recentProducts);

            return categoryInfo;
        }

        private JSONObject buildShoppingGender(JSONObject userBaseInfo) throws Exception {
            JSONObject shoppingGender = new JSONObject();
            shoppingGender.put("gender", userBaseInfo.getString("gender"));
            // 购物ID列表 - 基于所有历史数据
            shoppingGender.put("shoping_id", getUniqueProducts(allTimeShoppingState));
            return shoppingGender;
        }

        // ========== 统计计算方法 ==========

        private int getTodayLoginCount() throws Exception {
            Set<String> uniqueLoginTimes = new HashSet<>();
            String today = LocalDate.now().toString();

            // 只统计当天的登录，精确去重
            for (String loginTime : todayLoginState.get()) {
                if (loginTime.contains(today)) {
                    uniqueLoginTimes.add(loginTime);
                }
            }
            return uniqueLoginTimes.size();
        }

        private JSONArray calculatePreferredCategories() throws Exception {
            Map<String, Integer> categoryCount = new HashMap<>();
            Map<String, String> keywordToCategory = createCategoryMapping();

            // 基于所有历史搜索数据计算品类偏好
            for (String keyword : allTimeSearchState.get()) {
                String category = keywordToCategory.get(keyword);
                if (category != null) {
                    categoryCount.put(category, categoryCount.getOrDefault(category, 0) + 1);
                }
            }

            if (categoryCount.isEmpty()) {
                return new JSONArray(Arrays.asList("运动服饰", "配饰"));
            }

            List<String> topCategories = categoryCount.entrySet().stream()
                    .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                    .limit(2)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            return new JSONArray(topCategories);
        }

        private String calculateConsumptionLevel() throws Exception {
            // 基于所有历史消费金额计算消费等级
            Double totalSpend = totalSpendState.value();
            if (totalSpend == null || totalSpend == 0) return "unknown";
            if (totalSpend >= 2000) return "high";
            else if (totalSpend >= 500) return "midia";
            else return "lower";
        }

        // ========== 工具方法 ==========

        private JSONObject createDefaultUserInfo(String userId) {
            JSONObject defaultInfo = new JSONObject();
            defaultInfo.put("username", "用户_" + (userId != null ? userId.substring(0, Math.min(8, userId.length())) : "unknown"));
            defaultInfo.put("birthday", "1990-01-01");
            defaultInfo.put("age", "30");
            defaultInfo.put("constellation", "未知");
            defaultInfo.put("gender", "未知");
            defaultInfo.put("decade", "90后");
            defaultInfo.put("address", "未知");
            defaultInfo.put("phone_num", "未知");
            defaultInfo.put("year_best", "90");
            return defaultInfo;
        }

        private JSONObject createSensitiveWordRecord(JSONObject comment) {
            JSONObject record = new JSONObject();
            record.put("trigger_time", formatTimestamp(comment.getLong("ts")));
            record.put("trigger_word", comment.getString("triggered_keyword"));
            record.put("orderid", comment.getString("order_id"));
            record.put("sensitive_level", comment.getString("sensitive_level"));
            return record;
        }

        private JSONObject buildUserBaseInfo(JSONObject userBaseInfo) {
            JSONObject baseInfo = new JSONObject();
            baseInfo.put("birthday", userBaseInfo.getString("birthday"));
            baseInfo.put("decade", userBaseInfo.getString("decade"));
            baseInfo.put("gender", userBaseInfo.getString("gender"));
            baseInfo.put("zodiac_sign", userBaseInfo.getString("constellation"));
            baseInfo.put("age", userBaseInfo.getString("age"));
            baseInfo.put("address", userBaseInfo.getString("address"));
            baseInfo.put("phone_num", userBaseInfo.getString("phone_num"));
            return baseInfo;
        }

        private JSONArray buildTodayLoginTimes() throws Exception {
            List<String> times = new ArrayList<>();
            String today = LocalDate.now().toString();

            // 只返回当天的登录时间
            for (String time : todayLoginState.get()) {
                if (time.contains(today)) {
                    times.add(time);
                }
            }
            times.sort(String::compareTo);
            return new JSONArray(times);
        }

        private JSONArray buildDeviceInfo() throws Exception {
            JSONArray devices = new JSONArray();
            Set<String> deviceKeys = new HashSet<>();
            for (JSONObject device : deviceState.get()) {
                String key = device.getString("brand") + "_" + device.getString("device");
                if (!deviceKeys.contains(key)) {
                    devices.add(device);
                    deviceKeys.add(key);
                }
            }
            return devices;
        }

        private JSONArray buildSensitiveWords() throws Exception {
            JSONArray sensitiveWords = new JSONArray();
            for (JSONObject word : sensitiveWordState.get()) {
                sensitiveWords.add(word);
            }
            return sensitiveWords;
        }

        private boolean hasSensitiveWords() throws Exception {
            return sensitiveWordState.get().iterator().hasNext();
        }

        private Map<String, String> createCategoryMapping() {
            Map<String, String> mapping = new HashMap<>();
            mapping.put("运动内衣", "运动服饰");
            mapping.put("瑜伽服", "运动服饰");
            mapping.put("瑜伽裤", "运动服饰");
            mapping.put("运动套装", "运动服饰");
            mapping.put("背心", "运动服饰");
            mapping.put("休闲衫", "上装");
            mapping.put("羽绒", "外套");
            mapping.put("斜挎包", "配饰");
            mapping.put("运动", "运动服饰");
            mapping.put("瑜伽", "运动服饰");
            mapping.put("健身", "运动服饰");
            return mapping;
        }

        // ========== 基础工具方法 ==========

        private boolean isValidProductId(String productId) {
            return productId != null && !productId.trim().isEmpty() && !"null".equals(productId);
        }

        private String processProductId(String productId) {
            productId = productId.replace("\n", "").trim();
            if (productId.contains("item.jd.com")) {
                return extractProductIdFromUrl(productId);
            }
            return productId;
        }

        private boolean isValidKeyword(String keyword) {
            if (keyword == null || keyword.trim().isEmpty()) return false;
            String trimmed = keyword.trim().toLowerCase();
            return !FILTER_KEYWORDS.contains(trimmed) &&
                    trimmed.length() >= 1 &&
                    trimmed.length() <= 20 &&
                    !trimmed.matches("^\\d+$");
        }

        private String extractProductIdFromUrl(String url) {
            try {
                if (url.contains("/")) {
                    String lastPart = url.substring(url.lastIndexOf("/") + 1);
                    if (lastPart.contains(".")) {
                        return lastPart.split("\\.")[0];
                    }
                    return lastPart;
                }
                return url;
            } catch (Exception e) {
                return url;
            }
        }

        private int getUniqueCount(ListState<String> state) throws Exception {
            Set<String> unique = new HashSet<>();
            for (String item : state.get()) {
                if (item != null && !item.trim().isEmpty()) {
                    unique.add(item.trim());
                }
            }
            return unique.size();
        }

        private JSONArray getUniqueKeywords(ListState<String> state) throws Exception {
            Set<String> unique = new HashSet<>();
            for (String keyword : state.get()) {
                if (isValidKeyword(keyword)) {
                    unique.add(keyword);
                }
            }
            if (unique.isEmpty()) {
                return new JSONArray(Arrays.asList("运动服饰", "瑜伽裤"));
            }
            return new JSONArray(unique);
        }

        private JSONArray getUniqueProducts(ListState<String> state) throws Exception {
            Set<String> unique = new HashSet<>();
            for (String productId : state.get()) {
                if (isValidProductId(productId)) {
                    unique.add(productId);
                }
            }
            return new JSONArray(unique);
        }

        private String formatTimestamp(Long timestamp) {
            try {
                long adjusted = timestamp != null ? timestamp : System.currentTimeMillis();
                return Instant.ofEpochMilli(adjusted)
                        .atZone(ZoneId.of("Asia/Shanghai"))
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            } catch (Exception e) {
                return LocalDate.now().toString() + " 00:00:00";
            }
        }
    }
}