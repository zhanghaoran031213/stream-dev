package com.stream.realtime.lululemon.API2.func;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * @Author: ZHR
 * @Date: 2025/11/2 19:45
 * @Description: 设备统计指标 - 完整调试版本
 **/
public class DeviceStatsMetrics {

    /**
     * 计算设备统计信息 - 完整调试版本
     */
    public static void calculateDeviceStats(DataStream<JSONObject> filteredLogStream) {
        System.out.println("🚀 开始设备统计处理...");

        // 1. 首先验证数据源
        SingleOutputStreamOperator<String> sourceDebug = filteredLogStream
                .map(new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject log) throws Exception {
                        boolean hasDevice = hasValidDevice(log);
                        System.out.println("📥 接收到原始数据 - 有设备信息: " + hasDevice +
                                ", device: " + (log.containsKey("device") ? log.getJSONObject("device") : "null"));
                        return "原始数据验证: " + hasDevice;
                    }
                })
                .setParallelism(1);

        sourceDebug.print("原始数据验证");

        // 2. 统一设备统计流
        SingleOutputStreamOperator<Tuple2<String, Long>> unifiedDeviceStats = filteredLogStream
                .filter(log -> {
                    boolean valid = hasValidDevice(log);
                    if (valid) {
                        System.out.println("✅ 过滤有效设备数据: " + log.getJSONObject("device").toJSONString());
                    }
                    return valid;
                })
                .flatMap(new UnifiedDeviceStatsFlatMap())
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .name("device_flatmap");

        // 3. 调试输出：打印FlatMap后的数据
        SingleOutputStreamOperator<String> flatMapDebug = unifiedDeviceStats
                .map(new MapFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public String map(Tuple2<String, Long> value) throws Exception {
                        System.out.println("📤 FlatMap输出: " + value.f0 + " | count: " + value.f1);
                        return "FlatMap数据: " + value.f0 + " | " + value.f1;
                    }
                })
                .setParallelism(1);

        flatMapDebug.print("FlatMap调试");

        // 4. 窗口聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> windowedStats = unifiedDeviceStats
                .keyBy(tuple -> tuple.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30))) // 先用30秒窗口测试
                .sum(1)
                .name("window_aggregation");

        // 5. 调试输出：打印窗口聚合后的数据
        SingleOutputStreamOperator<String> windowDebug = windowedStats
                .map(new MapFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public String map(Tuple2<String, Long> value) throws Exception {
                        System.out.println("🎯 窗口聚合结果: " + value.f0 + " | 总数量: " + value.f1);
                        return "窗口聚合: " + value.f0 + " | " + value.f1;
                    }
                })
                .setParallelism(1);

        windowDebug.print("窗口聚合调试");

        // 6. 写入Doris
        windowedStats
                .addSink(com.stream.realtime.lululemon.API2.utils.DorisSinkUtils.createUnifiedDeviceStatsSink())
                .name("doris_unified_device_stats_sink")
                .setParallelism(1);

        System.out.println("✅ 设备统计处理流水线设置完成");
    }

    /**
     * 统一设备统计FlatMap函数 - 针对你的数据格式优化
     */
    public static class UnifiedDeviceStatsFlatMap implements FlatMapFunction<JSONObject, Tuple2<String, Long>> {
        @Override
        public void flatMap(JSONObject log, Collector<Tuple2<String, Long>> out) throws Exception {
            try {
                JSONObject device = log.getJSONObject("device");
                if (device == null) {
                    System.out.println("❌ 设备信息为空");
                    return;
                }

                // 提取时间戳
                Double timestamp = log.getDouble("ts");
                String dateStr = extractDateFromTimestamp(timestamp);
                System.out.println("⏰ 时间戳处理: " + timestamp + " -> 日期: " + dateStr);

                // 提取设备信息
                String plat = device.getString("plat");
                String brand = device.getString("brand");
                String platv = device.getString("platv");

                System.out.println("📱 原始设备数据 - plat: " + plat + ", brand: " + brand + ", platv: " + platv);

                // 标准化处理
                String osType = normalizeOS(plat);
                String normalizedBrand = normalizeBrand(brand);
                String version = (platv != null && !platv.isEmpty()) ? platv : "unknown";

                System.out.println("🔧 标准化后 - os: " + osType + ", brand: " + normalizedBrand + ", version: " + version);

                // 生成三种统计记录
                // 1. 操作系统统计
                String osKey = dateStr + "|os|" + osType + "|all|all";
                out.collect(Tuple2.of(osKey, 1L));
                System.out.println("✅ 生成OS统计: " + osKey);

                // 2. 品牌统计
                String brandKey = dateStr + "|brand|" + osType + "|" + normalizedBrand + "|all";
                out.collect(Tuple2.of(brandKey, 1L));
                System.out.println("✅ 生成品牌统计: " + brandKey);

                // 3. 版本统计
                String versionKey = dateStr + "|version|" + osType + "|" + normalizedBrand + "|" + version;
                out.collect(Tuple2.of(versionKey, 1L));
                System.out.println("✅ 生成版本统计: " + versionKey);

            } catch (Exception e) {
                System.err.println("❌ 设备数据处理异常: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * 检查是否有有效的设备信息
     */
    public static boolean hasValidDevice(JSONObject log) {
        if (!log.containsKey("device")) {
            return false;
        }

        JSONObject device = log.getJSONObject("device");
        if (device == null) {
            return false;
        }

        boolean hasPlat = device.containsKey("plat") && device.getString("plat") != null;
        boolean hasBrand = device.containsKey("brand") && device.getString("brand") != null;

        System.out.println("🔍 设备信息检查 - hasPlat: " + hasPlat + ", hasBrand: " + hasBrand);

        return hasPlat && hasBrand;
    }

    /**
     * 从时间戳提取日期字符串
     */
    public static String extractDateFromTimestamp(Double timestamp) {
        try {
            if (timestamp == null) {
                System.out.println("⚠️ 时间戳为空，使用当前日期");
                return LocalDate.now().toString();
            }

            System.out.println("⏰ 原始时间戳: " + timestamp);

            // 根据时间戳的大小判断单位
            long epochMillis;
            if (timestamp > 1e12) {
                // 如果时间戳大于 1e12（约2001年），认为是毫秒
                epochMillis = timestamp.longValue();
                System.out.println("⏰ 识别为毫秒时间戳");
            } else {
                // 否则认为是秒，转换为毫秒
                epochMillis = (long)(timestamp * 1000);
                System.out.println("⏰ 识别为秒时间戳，转换为毫秒: " + epochMillis);
            }

            Instant instant = Instant.ofEpochMilli(epochMillis);
            LocalDate date = instant.atZone(ZoneId.systemDefault()).toLocalDate();
            String result = date.toString();

            System.out.println("⏰ 最终日期: " + result);
            return result;

        } catch (Exception e) {
            System.err.println("❌ 时间戳解析失败: " + timestamp + ", 错误: " + e.getMessage());
            String currentDate = LocalDate.now().toString();
            System.out.println("⏰ 使用当前日期: " + currentDate);
            return currentDate;
        }
    }

    /**
     * 标准化操作系统名称
     */
    public static String normalizeOS(String plat) {
        if (plat == null || plat.isEmpty()) {
            return "Unknown";
        }
        String normalized = plat.toLowerCase().trim();
        System.out.println("🔧 标准化OS - 输入: " + plat + ", 标准化: " + normalized);

        if (normalized.contains("ios") || normalized.equals("iphone") || normalized.equals("ipad")) {
            return "iOS";
        } else if (normalized.contains("android")) {
            return "Android";
        } else {
            return "Other";
        }
    }

    /**
     * 标准化品牌名称
     */
    public static String normalizeBrand(String brand) {
        if (brand == null || brand.isEmpty()) {
            return "unknown";
        }

        String lowerBrand = brand.toLowerCase().trim();
        System.out.println("🔧 标准化品牌 - 输入: " + brand + ", 标准化: " + lowerBrand);

        if (lowerBrand.contains("apple") || lowerBrand.contains("iphone") || lowerBrand.contains("ipad")) {
            return "Apple";
        } else if (lowerBrand.contains("xiaomi") || lowerBrand.contains("mi") || lowerBrand.contains("redmi")) {
            return "Xiaomi";
        } else if (lowerBrand.contains("huawei") || lowerBrand.contains("honor")) {
            return "Huawei";
        } else if (lowerBrand.contains("samsung")) {
            return "Samsung";
        } else if (lowerBrand.contains("oppo")) {
            return "OPPO";
        } else if (lowerBrand.contains("vivo")) {
            return "VIVO";
        } else if (lowerBrand.contains("oneplus")) {
            return "OnePlus";
        } else if (lowerBrand.contains("meizu")) {
            return "Meizu";
        } else {
            String result = capitalizeFirst(brand);
            System.out.println("🔧 品牌默认标准化: " + brand + " -> " + result);
            return result;
        }
    }

    /**
     * 首字母大写
     */
    private static String capitalizeFirst(String str) {
        if (str == null || str.isEmpty()) {
            return "Unknown";
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1).toLowerCase();
    }
}