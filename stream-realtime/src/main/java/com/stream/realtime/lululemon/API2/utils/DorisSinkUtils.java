package com.stream.realtime.lululemon.API2.utils;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * @Author: ZHR
 * @Date: 2025/11/4 21:40
 * @Description: Doris Sink工具类 - 使用标准INSERT语法
 **/
public class DorisSinkUtils {

    // Doris 连接配置 - 使用 flink_analysis 数据库
    private static final String DORIS_URL = "jdbc:mysql://172.26.223.215:9030/flink_analysis";
    private static final String DORIS_USERNAME = "root";
    private static final String DORIS_PASSWORD = ""; // 如果没有密码就留空

    /**
     * 创建三级下钻热力图统计的 Doris Sink（省份→城市→运营商）- 增强过滤版
     */
    public static SinkFunction<org.apache.flink.api.java.tuple.Tuple3<String, String, Long>> createHeatMapDrilldownSink() {
        String sql = "INSERT INTO heat_map_drilldown_stats (ds, province, city, isp, visit_count) VALUES (?, ?, ?, ?, ?)";

        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<org.apache.flink.api.java.tuple.Tuple3<String, String, Long>>() {
                    @Override
                    public void accept(PreparedStatement ps, org.apache.flink.api.java.tuple.Tuple3<String, String, Long> record) throws SQLException {
                        try {
                            // record.f0: 日期, record.f1: 省份|城市|运营商, record.f2: 访问次数
                            String dateStr = record.f0;
                            String locationStr = record.f1;
                            Long visitCount = record.f2;

                            // 解析复合字段：省份|城市|运营商
                            String[] locationParts = locationStr.split("\\|");
                            String province, city, isp;

                            if (locationParts.length >= 3) {
                                province = locationParts[0].trim();
                                city = locationParts[1].trim();
                                isp = locationParts[2].trim();
                            } else if (locationParts.length == 2) {
                                province = locationParts[0].trim();
                                city = locationParts[1].trim();
                                isp = "未知运营商";
                            } else {
                                province = "未知";
                                city = "未知";
                                isp = "未知运营商";
                            }

                            // 处理日期
                            java.sql.Date date;
                            try {
                                date = java.sql.Date.valueOf(dateStr);
                            } catch (Exception e) {
                                System.err.println("❌ 日期格式错误: " + dateStr + ", 使用当前日期");
                                date = java.sql.Date.valueOf(java.time.LocalDate.now());
                            }

                            // 增强过滤逻辑：排除无效数据
                            if (!isValidProvince(province) || !isValidCity(city) || !isValidISP(isp)) {
                                System.out.println("🚫 跳过无效数据: " + province + "/" + city + "/" + isp + " | count: " + visitCount);
                                return; // 直接返回，不写入数据库
                            }

                            System.out.println("💾 写入三级下钻: " + date + " | " + province + " | " + city + " | " + isp + " | " + visitCount);

                            ps.setDate(1, date);
                            ps.setString(2, province);
                            ps.setString(3, city);
                            ps.setString(4, isp);
                            ps.setLong(5, visitCount);

                        } catch (Exception e) {
                            System.err.println("❌ 三级下钻数据写入失败: " + record + ", 错误: " + e.getMessage());
                        }
                    }

                    /**
                     * 检查省份是否有效
                     */
                    private boolean isValidProvince(String province) {
                        if (province == null || province.isEmpty()) return false;
                        // 排除国外地址、英文地址、拼音地址等
                        return !province.equals("未知") &&
                                !province.equals("内网") &&
                                !province.equals("国外") &&
                                !province.equals("数据库未加载") &&
                                !containsEnglish(province) &&
                                !isForeignLocation(province) &&
                                !isPinyinLocation(province);
                    }

                    /**
                     * 检查城市是否有效
                     */
                    private boolean isValidCity(String city) {
                        if (city == null || city.isEmpty()) return false;
                        // 排除国外地址、英文地址、拼音地址等
                        return !city.equals("未知") &&
                                !city.equals("内网") &&
                                !city.equals("国外") &&
                                !city.equals("数据库未加载") &&
                                !containsEnglish(city) &&
                                !isForeignLocation(city) &&
                                !isPinyinLocation(city);
                    }

                    /**
                     * 检查运营商是否有效
                     */
                    private boolean isValidISP(String isp) {
                        if (isp == null || isp.isEmpty()) return false;
                        return !isp.equals("未知运营商") &&
                                !isp.equals("内网") &&
                                !isp.equals("国外");
                    }

                    /**
                     * 检查是否包含英文字符
                     */
                    private boolean containsEnglish(String text) {
                        if (text == null) return false;
                        // 检查是否包含英文字母
                        return text.matches(".*[a-zA-Z].*");
                    }

                    /**
                     * 检查是否为国外地址
                     */
                    private boolean isForeignLocation(String location) {
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

                    /**
                     * 检查是否为拼音地址
                     */
                    private boolean isPinyinLocation(String location) {
                        if (location == null) return false;
                        String lowerLocation = location.toLowerCase();

                        // 常见的拼音地名模式
                        return lowerLocation.matches("^[a-z]+$") &&
                                location.length() > 2 &&
                                location.length() < 15;
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(5000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(DORIS_URL)
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(DORIS_USERNAME)
                        .withPassword(DORIS_PASSWORD)
                        .build()
        );
    }

    /**
     * 创建页面访问统计的 Doris Sink
     */
    public static SinkFunction<org.apache.flink.api.java.tuple.Tuple2<String, Long>> createPageViewSink() {
        String sql = "INSERT INTO page_view_stats (ds, page_name, pv) VALUES (?, ?, ?)";

        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<org.apache.flink.api.java.tuple.Tuple2<String, Long>>() {
                    @Override
                    public void accept(PreparedStatement ps, org.apache.flink.api.java.tuple.Tuple2<String, Long> record) throws SQLException {
                        // record.f0 是页面类型，record.f1 是访问量
                        ps.setDate(1, Date.valueOf(LocalDate.now())); // 当前日期作为 ds
                        ps.setString(2, record.f0); // 页面名称
                        ps.setLong(3, record.f1);   // 访问次数 pv
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(5000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(DORIS_URL)
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(DORIS_USERNAME)
                        .withPassword(DORIS_PASSWORD)
                        .build()
        );
    }

    /**
     * 创建搜索关键词统计的 Doris Sink（使用标准INSERT）
     */
    public static SinkFunction<org.apache.flink.api.java.tuple.Tuple3<String, String, Long>> createDailyKeywordSink() {
        String sql = "INSERT INTO search_keyword_stats (ds, keyword, search_count, update_time) VALUES (?, ?, ?, ?)";

        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<org.apache.flink.api.java.tuple.Tuple3<String, String, Long>>() {
                    @Override
                    public void accept(PreparedStatement ps, org.apache.flink.api.java.tuple.Tuple3<String, String, Long> record) throws SQLException {
                        // record.f0: 日期, record.f1: 关键词, record.f2: 搜索次数
                        ps.setDate(1, Date.valueOf(record.f0)); // 日期
                        ps.setString(2, record.f1); // 关键词
                        ps.setLong(3, record.f2);   // 搜索次数
                        ps.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now())); // 更新时间
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(5000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(DORIS_URL)
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(DORIS_USERNAME)
                        .withPassword(DORIS_PASSWORD)
                        .build()
        );
    }

    /**
     * 创建总体搜索关键词统计的 Doris Sink（使用标准INSERT）
     */
    public static SinkFunction<org.apache.flink.api.java.tuple.Tuple2<String, Long>> createTotalKeywordSink() {
        String sql = "INSERT INTO search_keyword_stats (ds, keyword, search_count, update_time) VALUES (?, ?, ?, ?)";

        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<org.apache.flink.api.java.tuple.Tuple2<String, Long>>() {
                    @Override
                    public void accept(PreparedStatement ps, org.apache.flink.api.java.tuple.Tuple2<String, Long> record) throws SQLException {
                        // record.f0: 关键词, record.f1: 搜索次数
                        ps.setDate(1, Date.valueOf(LocalDate.now())); // 当前日期
                        ps.setString(2, record.f0); // 关键词
                        ps.setLong(3, record.f1);   // 搜索次数
                        ps.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now())); // 更新时间
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(5000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(DORIS_URL)
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(DORIS_USERNAME)
                        .withPassword(DORIS_PASSWORD)
                        .build()
        );
    }

    /**
     * 创建热力图统计的 Doris Sink - 完整修复版
     */
    public static SinkFunction<org.apache.flink.api.java.tuple.Tuple3<String, String, Long>> createHeatMapSink() {
        String sql = "INSERT INTO heat_map_stats (ds, province, city, visit_count) VALUES (?, ?, ?, ?)";

        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<org.apache.flink.api.java.tuple.Tuple3<String, String, Long>>() {
                    @Override
                    public void accept(PreparedStatement ps, org.apache.flink.api.java.tuple.Tuple3<String, String, Long> record) throws SQLException {
                        try {
                            // record.f0: 日期, record.f1: 省份|城市, record.f2: 访问次数
                            String dateStr = record.f0.trim();
                            String locationStr = record.f1.trim();

                            // 解析省份和城市
                            String[] locationParts = locationStr.split("\\|");
                            String province, city;

                            if (locationParts.length >= 2) {
                                province = locationParts[0].trim();
                                city = locationParts[1].trim();
                            } else if (locationParts.length == 1) {
                                province = locationParts[0].trim();
                                city = province; // 如果没有城市，使用省份作为城市
                            } else {
                                province = "未知";
                                city = "未知";
                            }

                            // 验证日期格式
                            java.sql.Date date;
                            try {
                                date = java.sql.Date.valueOf(dateStr);
                            } catch (Exception e) {
                                System.err.println("❌ 日期格式错误: " + dateStr + ", 使用当前日期");
                                date = java.sql.Date.valueOf(LocalDate.now());
                            }

                            // 过滤无效数据
                            if (!isValidProvinceForHeatMap(province) || !isValidCityForHeatMap(city)) {
                                System.out.println("🚫 跳过无效热力图数据: " + province + "/" + city + " | count: " + record.f2);
                                return;
                            }

                            System.out.println("💾 写入热力图数据: ds=" + date + ", province=" + province + ", city=" + city + ", count=" + record.f2);

                            ps.setDate(1, date);
                            ps.setString(2, province);
                            ps.setString(3, city);
                            ps.setLong(4, record.f2);

                        } catch (Exception e) {
                            System.err.println("❌ 热力图数据写入失败: " + record + ", 错误: " + e.getMessage());
                            e.printStackTrace();
                            // 跳过错误数据，不抛出异常
                        }
                    }

                    /**
                     * 检查省份是否有效（热力图专用）
                     */
                    private boolean isValidProvinceForHeatMap(String province) {
                        if (province == null || province.isEmpty()) return false;
                        return !province.equals("未知") &&
                                !province.equals("内网") &&
                                !province.equals("国外") &&
                                !province.equals("数据库未加载") &&
                                !province.matches(".*[a-zA-Z].*");
                    }

                    /**
                     * 检查城市是否有效（热力图专用）
                     */
                    private boolean isValidCityForHeatMap(String city) {
                        if (city == null || city.isEmpty()) return false;
                        return !city.equals("未知") &&
                                !city.equals("内网") &&
                                !city.equals("国外") &&
                                !city.equals("数据库未加载") &&
                                !city.matches(".*[a-zA-Z].*");
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(5000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(DORIS_URL)
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(DORIS_USERNAME)
                        .withPassword(DORIS_PASSWORD)
                        .build()
        );
    }

    /**
     * 创建省份热力汇总的 Doris Sink
     */
    public static SinkFunction<org.apache.flink.api.java.tuple.Tuple3<String, String, Long>> createProvinceHeatSink() {
        String sql = "INSERT INTO province_heat_stats (ds, province, visit_count, city_count) VALUES (?, ?, ?, ?)";

        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<org.apache.flink.api.java.tuple.Tuple3<String, String, Long>>() {
                    @Override
                    public void accept(PreparedStatement ps, org.apache.flink.api.java.tuple.Tuple3<String, String, Long> record) throws SQLException {
                        try {
                            // record.f0: 日期, record.f1: 省份, record.f2: 访问次数
                            // city_count 需要另外计算，这里先设为1
                            String date = record.f0;
                            String province = record.f1;

                            // 过滤无效省份
                            if (!isValidProvinceForSummary(province)) {
                                System.out.println("🚫 跳过无效省份汇总数据: " + province + " | count: " + record.f2);
                                return;
                            }

                            System.out.println("📝 写入省份汇总数据: ds=" + date + ", province=" + province + ", count=" + record.f2);

                            ps.setDate(1, Date.valueOf(date));
                            ps.setString(2, province);
                            ps.setLong(3, record.f2);
                            ps.setInt(4, 1); // 每个记录代表1个城市
                        } catch (Exception e) {
                            System.err.println("❌ 省份汇总数据格式错误: " + record + ", 错误: " + e.getMessage());
                        }
                    }

                    /**
                     * 检查省份是否有效（汇总专用）
                     */
                    private boolean isValidProvinceForSummary(String province) {
                        if (province == null || province.isEmpty()) return false;
                        return !province.equals("未知") &&
                                !province.equals("内网") &&
                                !province.equals("国外") &&
                                !province.equals("数据库未加载") &&
                                !province.matches(".*[a-zA-Z].*");
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(5000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(DORIS_URL)
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(DORIS_USERNAME)
                        .withPassword(DORIS_PASSWORD)
                        .build()
        );
    }

    /**
     * 创建路径分析的 Doris Sink - 修复版（包含路径长度计算）
     */
    public static SinkFunction<org.apache.flink.api.java.tuple.Tuple2<String, Long>> createPathAnalysisSink() {
        // SQL：5个字段对应5个问号
        String sql = "INSERT INTO path_analysis_stats (ds, path_sequence, visit_count, path_length, update_time) VALUES (?, ?, ?, ?, ?)";

        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<org.apache.flink.api.java.tuple.Tuple2<String, Long>>() {
                    @Override
                    public void accept(PreparedStatement ps, org.apache.flink.api.java.tuple.Tuple2<String, Long> record) throws SQLException {
                        try {
                            // record.f0: 日期|路径序列, record.f1: 访问次数
                            String[] parts = record.f0.split("\\|", 2);
                            String date = parts[0];
                            String pathSequence = parts.length > 1 ? parts[1] : "unknown";

                            // 计算路径长度
                            int pathLength = calculatePathLength(pathSequence);

                            System.out.println("💾 写入路径数据: date=" + date +
                                    ", path=" + pathSequence +
                                    ", count=" + record.f1 +
                                    ", length=" + pathLength);

                            // 设置5个参数
                            ps.setDate(1, Date.valueOf(date));                    // ds
                            ps.setString(2, pathSequence);                       // path_sequence
                            ps.setLong(3, record.f1);                           // visit_count
                            ps.setInt(4, pathLength);                           // path_length
                            ps.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now())); // update_time

                        } catch (Exception e) {
                            System.err.println("❌ 路径数据写入失败: " + record + ", 错误: " + e.getMessage());
                            // 跳过错误数据，不抛出异常
                        }
                    }

                    /**
                     * 计算路径长度（路径中的行为数量）
                     */
                    private int calculatePathLength(String pathSequence) {
                        if (pathSequence == null || pathSequence.isEmpty()) {
                            return 0;
                        }
                        // 按 " -> " 分割，计算行为数量
                        String[] behaviors = pathSequence.split(" -> ");
                        return behaviors.length;
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(5000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(DORIS_URL)
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(DORIS_USERNAME)
                        .withPassword(DORIS_PASSWORD)
                        .build()
        );
    }

    /**
     * 创建TOP路径统计的 Doris Sink
     */
    public static SinkFunction<org.apache.flink.api.java.tuple.Tuple3<String, Integer, Long>> createTopPathSink() {
        String sql = "INSERT INTO top_path_stats (ds, rank, path_sequence, visit_count) VALUES (?, ?, ?, ?)";

        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<org.apache.flink.api.java.tuple.Tuple3<String, Integer, Long>>() {
                    @Override
                    public void accept(PreparedStatement ps, org.apache.flink.api.java.tuple.Tuple3<String, Integer, Long> record) throws SQLException {
                        // record.f0: 日期|路径序列, record.f1: 排名, record.f2: 访问次数
                        String[] parts = record.f0.split("\\|");
                        String date = parts[0];
                        String pathSequence = parts.length > 1 ? parts[1] : "unknown";

                        ps.setDate(1, Date.valueOf(date));
                        ps.setInt(2, record.f1); // 排名
                        ps.setString(3, pathSequence);
                        ps.setLong(4, record.f2);
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(5000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(DORIS_URL)
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(DORIS_USERNAME)
                        .withPassword(DORIS_PASSWORD)
                        .build()
        );
    }

    /**
     * 创建统一设备统计的 Doris Sink - 简化调试版本
     */
    public static SinkFunction<org.apache.flink.api.java.tuple.Tuple2<String, Long>> createUnifiedDeviceStatsSink() {
        String sql = "INSERT INTO unified_device_stats (ds, stats_type, os_type, brand, version, device_count, update_time) VALUES (?, ?, ?, ?, ?, ?, ?)";

        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<org.apache.flink.api.java.tuple.Tuple2<String, Long>>() {
                    @Override
                    public void accept(PreparedStatement ps, org.apache.flink.api.java.tuple.Tuple2<String, Long> record) throws SQLException {
                        try {
                            String data = record.f0;
                            Long count = record.f1;

                            System.out.println("🎯 开始处理Doris写入: " + data + " | count: " + count);

                            String[] parts = data.split("\\|");

                            if (parts.length < 5) {
                                System.err.println("❌ 数据格式错误，字段不足: " + data);
                                return;
                            }

                            String dateStr = parts[0];
                            String statsType = parts[1];
                            String osType = parts[2];
                            String brand = parts[3];
                            String version = parts[4];

                            // 简单日期处理
                            java.sql.Date sqlDate;
                            try {
                                sqlDate = java.sql.Date.valueOf(dateStr);
                                System.out.println("✅ 日期解析成功: " + sqlDate);
                            } catch (Exception e) {
                                System.err.println("❌ 日期解析失败: " + dateStr);
                                sqlDate = java.sql.Date.valueOf(LocalDate.now());
                            }

                            System.out.println("💾 准备写入Doris: " +
                                    "date=" + sqlDate +
                                    ", type=" + statsType +
                                    ", os=" + osType +
                                    ", brand=" + brand +
                                    ", version=" + version +
                                    ", count=" + count);

                            // 设置参数
                            ps.setDate(1, sqlDate);
                            ps.setString(2, statsType);
                            ps.setString(3, osType);
                            ps.setString(4, brand);
                            ps.setString(5, version);
                            ps.setLong(6, count);
                            ps.setTimestamp(7, Timestamp.valueOf(LocalDateTime.now()));

                            System.out.println("✅ SQL参数设置完成，准备执行插入");

                        } catch (Exception e) {
                            System.err.println("❌ Doris写入异常: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1)  // 先用1条测试
                        .withBatchIntervalMs(1000)
                        .withMaxRetries(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(DORIS_URL)
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(DORIS_USERNAME)
                        .withPassword(DORIS_PASSWORD)
                        .build()
        );
    }

    /**
     * 从记录中提取日期（辅助方法）
     */
    private static String extractDateFromRecord(org.apache.flink.api.java.tuple.Tuple3<String, String, Long> record) {
        // 这里可以根据实际数据结构调整日期提取逻辑
        // 如果记录中包含日期信息，可以从中提取
        // 目前先返回当前日期，实际使用时需要根据数据调整
        return LocalDate.now().toString();
    }
}