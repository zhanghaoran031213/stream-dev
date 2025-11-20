package com.stream.realtime.lululemon.API2;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonObject;
import com.stream.realtime.lululemon.API2.func.*;
import com.stream.realtime.lululemon.API2.utils.DorisSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import utils.ConfigUtils;
import utils.EnvironmentSettingUtils;
import utils.KafkaUtils;
import utils.WaterMarkUtils;


import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: ZHR
 * @Date: 2025/11/5 09:55
 * @Description:
 **/
public class DbusLogETLTask2 {

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

        SingleOutputStreamOperator<JsonObject> withLocation = kafkaLogDs
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
                .map(new IpLocationEnrichment())
                .name("ip-location-enrichment");



//        1. 历史天 + 当天 每个页面的总体访问量
//        ViewCountLogTest.calculatePageViewCount(filteredLogStream);


//        2. 历史天 + 当天 共计搜索词TOP10(每天的词云)
//        SearchKeywordMetrics.calculateDailyTop10Keywords(filteredLogStream); // 每日统计
//        SearchKeywordMetrics.calculateTotalTop10Keywords(filteredLogStream); // 总体统计
//        SearchKeywordMetrics.calculateRealTimeTop10Keywords(filteredLogStream); // 实时TOP10

        //        3. 历史天 + 当天 登陆区域的全国热力情况(每个地区的访问值)
//        HeatMapAnalysisUtils.analyzeHeatMap(kafkaLogDs);
        // 3. 三级下钻热力图分析（省份→城市→运营商）
        SingleOutputStreamOperator<Tuple3<String, String, Long>> heatMapDrilldownStream = withLocation
                .filter(json -> json.has("gis") && json.get("gis").isJsonObject())
                .filter(json -> {
                    JsonObject gis = json.getAsJsonObject("gis");
                    return gis.has("ip") && !gis.get("ip").isJsonNull();
                })
                .map(new MapFunction<JsonObject, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(JsonObject json) throws Exception {
                        try {
                            String ip = json.getAsJsonObject("gis").get("ip").getAsString();

                            // 使用增强的方法获取包含ISP的详细位置
                            String[] detailedLocation = IPLocationUtils.getDetailedLocationWithISP(ip);
                            String province = detailedLocation[0];
                            String city = detailedLocation[1];
                            String isp = detailedLocation[3];

                            // 从原始JSON中提取时间戳并转换为日期
                            Double timestamp = json.get("ts").getAsDouble();
                            String dateStr = extractDateFromTimestamp(timestamp);

                            // 增强过滤逻辑：排除未知、内网、国外地址
                            if (isValidLocation(province) && isValidLocation(city) && isValidISP(isp)) {
                                // 格式: Tuple3<日期, 省份|城市|运营商, 访问次数>
                                String locationKey = province + "|" + city + "|" + isp;
                                System.out.println("📍 三级下钻数据: " + dateStr + " | " + province + " | " + city + " | " + isp);
                                return Tuple3.of(dateStr, locationKey, 1L);
                            } else {
                                System.out.println("🚫 过滤无效位置: " + dateStr + " | " + province + " | " + city + " | " + isp);
                                return Tuple3.of(dateStr, "无效位置|无效位置|无效运营商", 0L);
                            }

                        } catch (Exception e) {
                            System.err.println("❌ 三级下钻数据处理失败: " + e.getMessage());
                            return Tuple3.of("1970-01-01", "未知|未知|未知运营商", 0L);
                        }
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .filter(tuple -> {
                    // 过滤有效数据：访问次数>0且位置信息有效
                    boolean valid = tuple.f2 > 0 && isValidLocationData(tuple.f1);
                    if (!valid) {
                        System.out.println("🚫 过滤无效数据: " + tuple);
                    } else {
                        // 额外检查是否包含英文字符
                        String[] parts = tuple.f1.split("\\|");
                        if (parts.length >= 2) {
                            String province = parts[0];
                            String city = parts[1];
                            if (containsEnglish(province) || containsEnglish(city)) {
                                System.out.println("🚫 过滤英文地址: " + tuple);
                                return false;
                            }
                        }
                    }
                    return valid;
                })
                .keyBy(tuple -> tuple.f0 + "|" + tuple.f1) // 按日期+位置作为key
                .sum(2)
                .name("heat-map-drilldown-stream");

        // 4. 运营商验证和统计
        SingleOutputStreamOperator<String> operatorVerification = heatMapDrilldownStream
                .map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) throws Exception {
                        String[] parts = value.f1.split("\\|");
                        String province = parts.length > 0 ? parts[0] : "未知";
                        String city = parts.length > 1 ? parts[1] : "未知";
                        String isp = parts.length > 2 ? parts[2] : "未知运营商";

                        // 统计运营商分布
                        if (isp.contains("联通")) {
                            System.out.println("🎯 发现联通数据: " + value.f0 + " | " + province + " | " + city + " | " + isp + " | " + value.f2);
                        } else if (isp.contains("移动")) {
                            System.out.println("📱 发现移动数据: " + value.f0 + " | " + province + " | " + city + " | " + isp + " | " + value.f2);
                        } else if (isp.contains("电信")) {
                            System.out.println("☎️ 发现电信数据: " + value.f0 + " | " + province + " | " + city + " | " + isp + " | " + value.f2);
                        }

                        return String.format("三级下钻统计 - 日期: %s | 省份: %s | 城市: %s | 运营商: %s | 访问量: %d",
                                value.f0, province, city, isp, value.f2);
                    }
                })
                .name("operator-verification");

        // 5. 运营商分布统计
        operatorVerification
                .keyBy(value -> {
                    // 提取运营商信息进行统计
                    if (value.contains("联通")) return "联通";
                    else if (value.contains("移动")) return "移动";
                    else if (value.contains("电信")) return "电信";
                    else return "其他";
                })
                .process(new KeyedProcessFunction<String, String, String>() {
                    private transient Map<String, Long> operatorCount;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        operatorCount = new HashMap<>();
                        operatorCount.put("联通", 0L);
                        operatorCount.put("移动", 0L);
                        operatorCount.put("电信", 0L);
                        operatorCount.put("其他", 0L);
                    }

                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        String operator = ctx.getCurrentKey();
                        operatorCount.put(operator, operatorCount.get(operator) + 1);

                        // 每处理10条数据输出一次统计
                        long total = operatorCount.values().stream().mapToLong(Long::longValue).sum();
                        if (total % 10 == 0) {
                            String stats = String.format("📊 运营商分布统计 - 联通: %d, 移动: %d, 电信: %d, 其他: %d, 总计: %d",
                                    operatorCount.get("联通"), operatorCount.get("移动"),
                                    operatorCount.get("电信"), operatorCount.get("其他"), total);
                            out.collect(stats);
                            System.out.println(stats);
                        }
                    }
                })
                .print("运营商分布统计");

        // 6. 写入三级下钻数据到Doris
        heatMapDrilldownStream
                .addSink(DorisSinkUtils.createHeatMapDrilldownSink())
                .name("doris_heat_map_drilldown_sink")
                .setParallelism(1);

        // 7. 调试输出
        heatMapDrilldownStream
                .map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) throws Exception {
                        String[] parts = value.f1.split("\\|");
                        String province = parts.length > 0 ? parts[0] : "未知";
                        String city = parts.length > 1 ? parts[1] : "未知";
                        String isp = parts.length > 2 ? parts[2] : "未知运营商";

                        return String.format("三级下钻统计 - 日期: %s | 省份: %s | 城市: %s | 运营商: %s | 访问量: %d",
                                value.f0, province, city, isp, value.f2);
                    }
                })
                .print("三级下钻热力图分析");

        // 8. 执行环境
        env.execute("DbusLogETLTask2 - 三级下钻热力图分析");

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

//        SingleOutputStreamOperator<Tuple2<String, String>> userPaths = filteredLogStream
//                .filter(log -> {
//                    // 过滤条件：必须有user_id，并且log_type是业务页面
//                    boolean hasUserId = log.containsKey("user_id") &&
//                            log.getString("user_id") != null &&
//                            !log.getString("user_id").isEmpty();
//
//                    String logType = log.getString("log_type");
//                    boolean isBusinessPage = logType != null && (
//                            logType.equals("search") ||
//                                    logType.equals("home") ||
//                                    logType.equals("product_list") ||
//                                    logType.equals("login") ||
//                                    logType.equals("product_detail") ||
//                                    logType.equals("payment")
//                    );
//
//                    if (hasUserId && isBusinessPage) {
//                        System.out.println("✅ Found business page: " + logType + " for user: " + log.getString("user_id"));
//                    }
//
//                    return hasUserId && isBusinessPage;
//                })
//                .map(log -> {
//                    String userId = log.getString("user_id");
//                    String logType = log.getString("log_type"); // 使用log_type作为页面标识
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
//                    System.out.println("📝 Processing business path - User: " + userId +
//                            " | Date: " + dateStr + " | LogType: " + logType);
//
//                    return Tuple2.of(userId + "|" + dateStr, logType + "|" + timestamp);
//                })
//                .returns(Types.TUPLE(Types.STRING, Types.STRING))
//                .filter(tuple -> !"INVALID".equals(tuple.f0))
//                .name("business-user-paths-extraction");
//
//        // 业务路径分析处理链
//        SingleOutputStreamOperator<Tuple2<String, Long>> pathAnalysisStream = userPaths
//                .keyBy(tuple -> tuple.f0)
//                .process(new UserPathAnalysisFunction())
//                .map(value -> {
//                    String date = value.f0;
//                    String pathSequence = value.f1;
//                    return Tuple2.of(date + "|" + pathSequence, 1L);
//                })
//                .returns(Types.TUPLE(Types.STRING, Types.LONG))
//                .keyBy(tuple -> tuple.f0)
//                .sum(1)
//                .name("path-analysis-aggregation");
//
//        // 写入路径分析数据到Doris
//        pathAnalysisStream
//                .addSink(DorisSinkUtils.createPathAnalysisSink())
//                .name("doris_path_analysis_sink")
//                .setParallelism(1);
//
//        // 同时输出到控制台用于调试
//        pathAnalysisStream
//                .map(new MapFunction<Tuple2<String, Long>, String>() {
//                    @Override
//                    public String map(Tuple2<String, Long> value) throws Exception {
//                        String[] parts = value.f0.split("\\|", 2);
//                        String date = parts[0];
//                        String path = parts.length > 1 ? parts[1] : "unknown";
//                        return String.format("Business Path Analysis - Date: %s | Path: %s | Count: %d", date, path, value.f1);
//                    }
//                })
//                .print("Business Path Analysis Results");


        // 5. 历史天 + 当天 用户设备的统计(ios & android (子类品牌(版本))) 下钻 (饼图 & 玫瑰图)
//        DeviceStatsMetrics.calculateDeviceStats(filteredLogStream);

//        SingleOutputStreamOperator<Tuple2<String, Long>> deviceOSStream = filteredLogStream
//                .filter(log -> DeviceStatsMetrics.hasValidDevice(log))
//                .map(log -> {
//                    JSONObject device = log.getJSONObject("device");
//                    String plat = device.getString("plat");
//                    Double timestamp = log.getDouble("ts");
//                    String dateStr = DeviceStatsMetrics.extractDateFromTimestamp(timestamp);
//
//                    return Tuple2.of(dateStr + "|" + DeviceStatsMetrics.normalizeOS(plat), 1L);
//                })
//                .returns(Types.TUPLE(Types.STRING, Types.LONG))
//                .keyBy(tuple -> tuple.f0)
//                .sum(1)
//                .name("device-os-stream");
//
//        // 品牌统计
//        SingleOutputStreamOperator<Tuple2<String, Long>> deviceBrandStream = filteredLogStream
//                .filter(log -> DeviceStatsMetrics.hasValidDevice(log))
//                .map(log -> {
//                    JSONObject device = log.getJSONObject("device");
//                    String plat = device.getString("plat");
//                    String brand = device.getString("brand");
//                    Double timestamp = log.getDouble("ts");
//                    String dateStr = DeviceStatsMetrics.extractDateFromTimestamp(timestamp);
//
//                    String osType = DeviceStatsMetrics.normalizeOS(plat);
//                    String normalizedBrand = DeviceStatsMetrics.normalizeBrand(brand);
//
//                    return Tuple2.of(dateStr + "|" + osType + "|" + normalizedBrand, 1L);
//                })
//                .returns(Types.TUPLE(Types.STRING, Types.LONG))
//                .keyBy(tuple -> tuple.f0)
//                .sum(1)
//                .name("device-brand-stream");
//
//        // 品牌版本统计
//        SingleOutputStreamOperator<Tuple2<String, Long>> deviceBrandVersionStream = filteredLogStream
//                .filter(log -> DeviceStatsMetrics.hasValidDevice(log))
//                .map(log -> {
//                    JSONObject device = log.getJSONObject("device");
//                    String plat = device.getString("plat");
//                    String brand = device.getString("brand");
//                    String platv = device.getString("platv");
//                    Double timestamp = log.getDouble("ts");
//                    String dateStr = DeviceStatsMetrics.extractDateFromTimestamp(timestamp);
//
//                    String osType = DeviceStatsMetrics.normalizeOS(plat);
//                    String normalizedBrand = DeviceStatsMetrics.normalizeBrand(brand);
//                    String version = platv != null && !platv.isEmpty() ? platv : "unknown";
//
//                    return Tuple2.of(dateStr + "|" + osType + "|" + normalizedBrand + "|" + version, 1L);
//                })
//                .returns(Types.TUPLE(Types.STRING, Types.LONG))
//                .keyBy(tuple -> tuple.f0)
//                .sum(1)
//                .name("device-brand-version-stream");
//
//        // 写入设备统计数据到Doris
//        deviceOSStream
//                .addSink(DorisSinkUtils.createDeviceOSStatsSink())
//                .name("doris_device_os_sink")
//                .setParallelism(1);
//
//        deviceBrandStream
//                .addSink(DorisSinkUtils.createDeviceBrandStatsSink())
//                .name("doris_device_brand_sink")
//                .setParallelism(1);
//
//        deviceBrandVersionStream
//                .addSink(DorisSinkUtils.createDeviceBrandVersionStatsSink())
//                .name("doris_device_brand_version_sink")
//                .setParallelism(1);


        //6. 画像 每个用户 登陆的天数(分别是多少号)，在登陆期间 是否有购买行为，是否有搜索行为，是否有浏览行为，登陆时间是什么时间段 -> json -> es
//        UserProfileAnalysis.calculateUserProfile(filteredLogStream);


        env.execute();
    }

    /**
     * 从时间戳提取日期字符串
     */
    private static String extractDateFromTimestamp(Double timestamp) {
        try {
            long ts = (timestamp > 1e12) ? timestamp.longValue() : (long)(timestamp * 1000);
            return java.time.Instant.ofEpochMilli(ts)
                    .atZone(java.time.ZoneId.systemDefault())
                    .toLocalDate()
                    .toString();
        } catch (Exception e) {
            System.err.println("❌ 时间戳转换失败: " + timestamp + ", 使用当前日期");
            return java.time.LocalDate.now().toString();
        }
    }

    /**
     * 检查位置信息是否有效（排除未知、内网、国外地址）
     */
    private static boolean isValidLocation(String location) {
        if (location == null) return false;
        return !"未知".equals(location) &&
                !"内网".equals(location) &&
                !"国外".equals(location) &&
                !"数据库未加载".equals(location) &&
                !containsEnglish(location);
    }

    /**
     * 检查运营商信息是否有效
     */
    private static boolean isValidISP(String isp) {
        if (isp == null) return false;
        return !"未知运营商".equals(isp) &&
                !"内网".equals(isp) &&
                !"国外".equals(isp);
    }

    /**
     * 检查位置数据是否有效
     */
    private static boolean isValidLocationData(String locationData) {
        if (locationData == null) return false;
        String[] parts = locationData.split("\\|");
        if (parts.length < 3) return false;

        String province = parts[0];
        String city = parts[1];
        String isp = parts[2];

        return isValidLocation(province) &&
                isValidLocation(city) &&
                isValidISP(isp);
    }

    /**
     * 检查是否包含英文字符
     */
    private static boolean containsEnglish(String text) {
        if (text == null) return false;
        return text.matches(".*[a-zA-Z].*");
    }

    /**
     * 检查是否为国外地址
     */
    private static boolean isForeignLocation(String location) {
        if (location == null) return false;
        String lowerLocation = location.toLowerCase();

        // 常见的国外地区标识
        return lowerLocation.contains("central") ||
                lowerLocation.contains("western") ||
                lowerLocation.contains("district") ||
                lowerLocation.contains("new south wales") ||
                lowerLocation.contains("sydney") ||
                lowerLocation.contains("hwang") ||
                lowerLocation.contains("chow") ||
                lowerLocation.contains("lianyun") ||
                lowerLocation.contains("tokyo") ||
                lowerLocation.contains("seoul") ||
                lowerLocation.contains("london") ||
                lowerLocation.contains("paris") ||
                lowerLocation.contains("new york") ||
                lowerLocation.contains("los angeles") ||
                lowerLocation.contains("california") ||
                lowerLocation.contains("texas") ||
                lowerLocation.contains("florida") ||
                lowerLocation.contains("washington") ||
                lowerLocation.contains("boston") ||
                lowerLocation.contains("chicago") ||
                lowerLocation.contains("moscow") ||
                lowerLocation.contains("berlin") ||
                lowerLocation.contains("rome") ||
                lowerLocation.contains("madrid") ||
                lowerLocation.contains("amsterdam") ||
                lowerLocation.contains("vancouver") ||
                lowerLocation.contains("toronto") ||
                lowerLocation.contains("melbourne") ||
                lowerLocation.contains("auckland");
    }

}
