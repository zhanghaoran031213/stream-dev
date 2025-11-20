// -------------- 完整修正版本 -----------------

package com.stream.realtime.lululemon.API3;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import utils.ConfigUtils;
import utils.EnvironmentSettingUtils;
import utils.WaterMarkUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class DbusLogAssociationSysData2kafka {

    private static final String KAFKA_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String TOPIC_LOG = "realtime_v3_logs";
    private static final String TOPIC_COMMENT = "realtime_v3_comment_cdc";

    private static final String HBASE_TABLE = "realtime_v3:dim_user_info_base_Constellation";
    private static final String CF = "info";

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        env.setStateBackend(new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                new org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage()
        );

        // logs
        DataStreamSource<String> logsDs = env.fromSource(
                KafkaSource.<String>builder()
                        .setBootstrapServers(KAFKA_SERVERS)
                        .setTopics(TOPIC_LOG)
                        .setGroupId("g_logs_" + System.currentTimeMillis())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .build(),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5),
                "_src_logs"
        );

        // comment CDC
        DataStreamSource<String> commentDs = env.fromSource(
                KafkaSource.<String>builder()
                        .setBootstrapServers(KAFKA_SERVERS)
                        .setTopics(TOPIC_COMMENT)
                        .setGroupId("g_comment_" + System.currentTimeMillis())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .build(),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5),
                "_src_comment"
        );

        SingleOutputStreamOperator<JSONObject> logJson = logsDs.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> commentJson = commentDs.map(JSON::parseObject);

        // enrich logs with HBase dims
        SingleOutputStreamOperator<JSONObject> logEnriched =
                logJson.map(new HBaseEnrichFunction());

        // interval join
        SingleOutputStreamOperator<JSONObject> finalResult =
                logEnriched.keyBy(x -> x.getString("user_id"))
                        .intervalJoin(commentJson.keyBy(x -> x.getString("user_id")))
                        .between(Time.seconds(-180), Time.seconds(180))
                        .process(new JoinFunctionFinal());

        finalResult.print();

        env.execute("DbusLogAssociationSysData2kafka_FinalVersion");
    }

    // =======================================================================
    // JOIN 逻辑 —— 修正 login_time 处理
    // =======================================================================
    public static class JoinFunctionFinal extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {

        private transient MapState<String, JSONObject> deviceState;
        private transient MapState<String, Long> dailyLoginState; // 改为 <日期, 最后登录时间戳>
        private transient MapState<String, Boolean> shoppingState;
        private transient MapState<String, Double> consumptionState;

        // 新增：记录当天最后处理时间，用于10分钟间隔控制
        private transient ValueState<Long> lastProcessTimeState;

        @Override
        public void open(Configuration parameters) {
            deviceState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("deviceState", String.class, JSONObject.class));

            dailyLoginState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("dailyLoginState", String.class, Long.class));

            shoppingState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("shoppingState", String.class, Boolean.class));

            consumptionState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("consumptionState", String.class, Double.class));

            lastProcessTimeState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastProcessTimeState", Long.class));
        }

        @Override
        public void processElement(JSONObject log,
                                   JSONObject comment,
                                   Context ctx,
                                   Collector<JSONObject> out) throws Exception {

            JSONObject result = new JSONObject();
            String userId = log.getString("user_id");
            result.put("user_id", userId);

            Long currentTs = log.getLong("ts");
            LocalDate currentDate = Instant.ofEpochSecond(currentTs)
                    .atZone(ZoneId.of("Asia/Shanghai"))
                    .toLocalDate();
            String dateStr = currentDate.toString();

            // ---------------- login_time 列表处理 ----------------
            updateLoginTime(currentTs, currentDate);
            JSONArray loginArr = buildLoginTimeArray(currentDate);
            result.put("login_time", loginArr);

            // ---------------- device_info 多条累加 ----------------
            JSONObject deviceObj = log.getJSONObject("device_info_single");
            if (deviceObj != null) {
                String stableKey = sortedKey(deviceObj);
                if (!deviceState.contains(stableKey)) {
                    deviceState.put(stableKey, deviceObj);
                }
            }

            JSONArray devArr = new JSONArray();
            for (JSONObject d : deviceState.values()) devArr.add(d);
            result.put("device_info", devArr);

            // ---------------- search_info ----------------
            result.put("search_info", log.getJSONArray("search_info"));

            // ---------------- category_info ----------------
            result.put("category_info", log.getJSONObject("category_info"));

            // ---------------- shoping_gender + 累加商品id ----------------
            JSONObject userBaseInfo = log.getJSONObject("user_base_info");
            String gender = userBaseInfo != null ? userBaseInfo.getString("gender") : null;
            JSONObject shopGender = buildShoppingGender(gender, log, shoppingState);
            result.put("shoping_gender", shopGender);

            // =====================================================================
            // ==========================   评论数据补齐   ===========================
            // =====================================================================

            // 评论敏感词
            String sensitiveLevel = comment.getString("sensitive_level");
            boolean isSensitive = !"CLEAN".equalsIgnoreCase(sensitiveLevel);
            result.put("is_check_sensitive_comment", isSensitive ? "1" : "0");

            JSONArray sensitiveArr = new JSONArray();
            if (isSensitive) {
                JSONObject hit = new JSONObject();
                hit.put("trigger_time", comment.getString("ds"));
                hit.put("trigger_word", comment.getString("triggered_keyword"));
                hit.put("orderid", comment.getString("order_id"));
                sensitiveArr.add(hit);
            }
            result.put("sensitive_word", sensitiveArr);

            // 评论相关字段
            result.put("comment_order_id", comment.getString("order_id"));
            result.put("comment_text", comment.getString("user_comment"));
            result.put("comment_ds", comment.getString("ds"));
            result.put("comment_sensitive_level", sensitiveLevel);
            result.put("comment_trigger_keyword", comment.getString("triggered_keyword"));

            Double totalAmount = comment.getDouble("total_amount");
            result.put("comment_total_amount", totalAmount);

            // ---------------- 消费等级 ----------------
            updateConsumptionState(comment);
            double totalConsumption = calculateTotalConsumption();
            result.put("consumption_level", calcLevel(totalConsumption));
            result.put("total_consumption_amount", totalConsumption);

            // ---------------- 用户基本信息 ----------------
            result.put("username", log.getString("username"));
            result.put("user_base_info", userBaseInfo);

            // ---------------- ds & ts ----------------
            result.put("ds", dateStr);
            result.put("ts", currentTs);

            // 添加分区字段 pt
            String pt = generatePartition(userId);
            result.put("pt", pt);

            // 检查是否需要输出（10分钟间隔控制）
            if (shouldOutput(currentTs)) {
                out.collect(result);
                lastProcessTimeState.update(currentTs);
            }
        }

        /**
         * 更新登录时间状态
         * 历史天：每天只保留最后一条登录时间
         * 当天：实时更新
         */
        private void updateLoginTime(Long currentTs, LocalDate currentDate) throws Exception {
            String dateStr = currentDate.toString();
            Long existingTs = dailyLoginState.get(dateStr);

            // 如果是当天或者是新的日期，更新登录时间
            if (existingTs == null || currentTs > existingTs) {
                dailyLoginState.put(dateStr, currentTs);
            }
        }

        /**
         * 构建登录时间数组
         * 历史天：每天只输出最后一条登录时间
         * 当天：输出当天所有的登录时间（按10分钟间隔）
         */
        private JSONArray buildLoginTimeArray(LocalDate currentDate) throws Exception {
            JSONArray loginArr = new JSONArray();

            for (String dateStr : dailyLoginState.keys()) {
                LocalDate date = LocalDate.parse(dateStr);
                Long loginTs = dailyLoginState.get(dateStr);

                if (date.equals(currentDate)) {
                    // 当天：输出当前时间（10分钟间隔控制）
                    loginArr.add(loginTs);
                } else {
                    // 历史天：每天只输出最后一条登录时间
                    loginArr.add(loginTs);
                }
            }

            // 按时间戳排序（最新的在前）
            loginArr.sort((a, b) -> Long.compare((Long)b, (Long)a));
            return loginArr;
        }

        /**
         * 判断是否需要输出（10分钟间隔控制）
         */
        private boolean shouldOutput(Long currentTs) throws Exception {
            Long lastProcessTime = lastProcessTimeState.value();
            if (lastProcessTime == null) {
                return true; // 第一次处理，需要输出
            }

            // 计算时间差（分钟）
            long timeDiffMinutes = (currentTs - lastProcessTime) / 60;
            return timeDiffMinutes >= 10; // 10分钟间隔
        }

        private JSONObject buildShoppingGender(String gender,
                                               JSONObject log,
                                               MapState<String, Boolean> shoppingState) throws Exception {
            String pid = log.getString("product_id");
            if (pid != null && !shoppingState.contains(pid)) {
                shoppingState.put(pid, true);
            }

            JSONArray arr = new JSONArray();
            for (String id : shoppingState.keys()) arr.add(id);

            JSONObject obj = new JSONObject();
            obj.put("gender", gender);
            obj.put("shoping_id", arr);
            return obj;
        }

        private void updateConsumptionState(JSONObject comment) throws Exception {
            String orderId = comment.getString("order_id");
            Double amount = comment.getDouble("total_amount");
            if (orderId != null && amount != null && !consumptionState.contains(orderId)) {
                consumptionState.put(orderId, amount);
            }
        }

        private double calculateTotalConsumption() throws Exception {
            double total = 0.0;
            for (Double amount : consumptionState.values()) {
                total += amount;
            }
            return total;
        }

        private String sortedKey(JSONObject obj) {
            TreeMap<String, String> m = new TreeMap<>();
            for (String k : obj.keySet()) m.put(k, obj.getString(k));
            return m.toString();
        }

        private String generatePartition(String userId) {
            if (userId == null) return "pt=00";
            int partition = Math.abs(userId.hashCode()) % 100;
            return String.format("pt=%02d", partition);
        }
    }

    // =======================================================================
    // HBase 补充维度（保持不变）
    // =======================================================================
    public static class HBaseEnrichFunction extends RichMapFunction<JSONObject, JSONObject> {

        private transient Connection connection;
        private transient Table table;

        @Override
        public void open(Configuration parameters) throws Exception {
            org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
            config.set("hbase.zookeeper.property.clientPort", "2181");
            config.set("zookeeper.znode.parent", "/hbase");

            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(HBASE_TABLE));
        }

        @Override
        public JSONObject map(JSONObject log) throws Exception {

            String userId = log.getString("user_id");

            JSONObject dim = new JSONObject();
            if (userId != null) {
                Get get = new Get(Bytes.toBytes(userId));
                get.addFamily(Bytes.toBytes(CF));
                Result r = table.get(get);

                if (!r.isEmpty()) {
                    dim.put("uname", val(r, "uname"));
                    dim.put("birthday", val(r, "birthday"));
                    dim.put("gender", val(r, "gender"));
                    dim.put("address", val(r, "address"));
                    dim.put("age", val(r, "age"));
                    dim.put("year_best", val(r, "year_best"));
                    dim.put("constellation", val(r, "constellation"));
                }
            }

            // username
            log.put("username", dim.getString("uname"));

            // user_base_info
            JSONObject base = new JSONObject();
            base.put("birthday", dim.getString("birthday"));
            base.put("decade", decade(dim.getString("birthday")));
            base.put("gender", dim.getString("gender"));
            base.put("zodiac_sign", dim.getString("constellation"));
            base.put("address", dim.getString("address"));
            base.put("age", dim.getString("age"));
            base.put("year_best", dim.getString("year_best"));
            log.put("user_base_info", base);

            // device
            log.put("device_info_single", log.getJSONObject("device"));

            // search
            log.put("search_info", log.getJSONArray("keywords"));

            // category
            JSONObject cate = new JSONObject();
            cate.put("product_id", log.getString("product_id"));
            cate.put("order_id", log.getString("order_id"));
            cate.put("opa", log.getString("opa"));
            log.put("category_info", cate);

            return log;
        }

        private String val(Result r, String col) {
            byte[] v = r.getValue(Bytes.toBytes(CF), Bytes.toBytes(col));
            return v == null ? null : Bytes.toString(v);
        }

        private String decade(String birthday) {
            if (birthday == null || birthday.length() < 4) return "";
            return birthday.substring(0, 3) + "0年代";
        }

        @Override
        public void close() throws Exception {
            if (table != null) table.close();
            if (connection != null) connection.close();
        }
    }

    private static String calcLevel(Double amount) {
        if (amount == null) return "low";
        if (amount >= 5000) return "premium";
        else if (amount >= 2000) return "high";
        else if (amount >= 500) return "mid";
        else return "low";
    }
}