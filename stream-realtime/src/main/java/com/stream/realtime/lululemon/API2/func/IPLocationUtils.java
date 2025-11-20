// IPLocationUtils.java (使用 GeoLite2) - 完整修复版
package com.stream.realtime.lululemon.API2.func;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Subdivision;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * IP地理位置工具类 - 使用GeoLite2数据库
 */
public class IPLocationUtils {

    private static final Logger logger = LoggerFactory.getLogger(IPLocationUtils.class);

    private static DatabaseReader dbReader;
    private static final Map<String, String> LOCATION_CACHE = new ConcurrentHashMap<>();
    private static boolean dbLoaded = false;

    // 翻译映射表
    private static final Map<String, String> PROVINCE_TRANSLATION_MAP = new HashMap<>();
    private static final Map<String, String> CITY_TRANSLATION_MAP = new HashMap<>();
    private static final Map<String, String> COUNTRY_TRANSLATION_MAP = new HashMap<>();

    static {
        logger.info("IPLocationUtils静态初始化开始");
        initTranslationMaps();
        initDatabase();
        logger.info("IPLocationUtils静态初始化结束，dbLoaded={}", dbLoaded);
    }

    /**
     * 初始化翻译映射表
     */
    private static void initTranslationMaps() {
        // 国家翻译
        COUNTRY_TRANSLATION_MAP.put("china", "中国");
        COUNTRY_TRANSLATION_MAP.put("united states", "美国");
        COUNTRY_TRANSLATION_MAP.put("japan", "日本");
        COUNTRY_TRANSLATION_MAP.put("korea", "韩国");

        // 省份翻译
        PROVINCE_TRANSLATION_MAP.put("beijing", "北京");
        PROVINCE_TRANSLATION_MAP.put("shanghai", "上海");
        PROVINCE_TRANSLATION_MAP.put("tianjin", "天津");
        PROVINCE_TRANSLATION_MAP.put("chongqing", "重庆");
        PROVINCE_TRANSLATION_MAP.put("hebei", "河北");
        PROVINCE_TRANSLATION_MAP.put("shanxi", "山西");
        PROVINCE_TRANSLATION_MAP.put("liaoning", "辽宁");
        PROVINCE_TRANSLATION_MAP.put("jilin", "吉林");
        PROVINCE_TRANSLATION_MAP.put("heilongjiang", "黑龙江");
        PROVINCE_TRANSLATION_MAP.put("jiangsu", "江苏");
        PROVINCE_TRANSLATION_MAP.put("zhejiang", "浙江");
        PROVINCE_TRANSLATION_MAP.put("anhui", "安徽");
        PROVINCE_TRANSLATION_MAP.put("fujian", "福建");
        PROVINCE_TRANSLATION_MAP.put("jiangxi", "江西");
        PROVINCE_TRANSLATION_MAP.put("shandong", "山东");
        PROVINCE_TRANSLATION_MAP.put("henan", "河南");
        PROVINCE_TRANSLATION_MAP.put("hubei", "湖北");
        PROVINCE_TRANSLATION_MAP.put("hunan", "湖南");
        PROVINCE_TRANSLATION_MAP.put("guangdong", "广东");
        PROVINCE_TRANSLATION_MAP.put("hainan", "海南");
        PROVINCE_TRANSLATION_MAP.put("sichuan", "四川");
        PROVINCE_TRANSLATION_MAP.put("guizhou", "贵州");
        PROVINCE_TRANSLATION_MAP.put("yunnan", "云南");
        PROVINCE_TRANSLATION_MAP.put("shaanxi", "陕西");
        PROVINCE_TRANSLATION_MAP.put("gansu", "甘肃");

        // 城市翻译
        CITY_TRANSLATION_MAP.put("beijing", "北京");
        CITY_TRANSLATION_MAP.put("shanghai", "上海");
        CITY_TRANSLATION_MAP.put("guangzhou", "广州");
        CITY_TRANSLATION_MAP.put("shenzhen", "深圳");
        CITY_TRANSLATION_MAP.put("hangzhou", "杭州");
        CITY_TRANSLATION_MAP.put("nanjing", "南京");
        CITY_TRANSLATION_MAP.put("wuhan", "武汉");
        CITY_TRANSLATION_MAP.put("chengdu", "成都");
        CITY_TRANSLATION_MAP.put("xian", "西安");
        CITY_TRANSLATION_MAP.put("tianjin", "天津");
        CITY_TRANSLATION_MAP.put("chongqing", "重庆");
        CITY_TRANSLATION_MAP.put("dalian", "大连");
        CITY_TRANSLATION_MAP.put("qingdao", "青岛");
        CITY_TRANSLATION_MAP.put("ningbo", "宁波");
        CITY_TRANSLATION_MAP.put("xiamen", "厦门");
        CITY_TRANSLATION_MAP.put("suzhou", "苏州");
        CITY_TRANSLATION_MAP.put("wuxi", "无锡");
        CITY_TRANSLATION_MAP.put("foshan", "佛山");
        CITY_TRANSLATION_MAP.put("dongguan", "东莞");
        CITY_TRANSLATION_MAP.put("zhuhai", "珠海");
        CITY_TRANSLATION_MAP.put("nanchang", "南昌");
        CITY_TRANSLATION_MAP.put("jinan", "济南");
        CITY_TRANSLATION_MAP.put("tangshan", "唐山");
        CITY_TRANSLATION_MAP.put("kunming", "昆明");
    }

    /**
     * 初始化数据库
     */
    @SneakyThrows
    private static void initDatabase() {
        try {
            logger.info("开始加载IP数据库 (GeoLite2)");
            logger.info("当前工作目录: {}", System.getProperty("user.dir"));

            // 从类路径加载
            InputStream inputStream = IPLocationUtils.class.getClassLoader()
                    .getResourceAsStream("GeoLite2-City.mmdb");

            if (inputStream != null) {
                logger.info("从类路径找到数据库文件");
                File tempFile = File.createTempFile("GeoLite2-City", ".mmdb");
                tempFile.deleteOnExit();
                java.nio.file.Files.copy(inputStream, tempFile.toPath(),
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);

                logger.info("临时文件路径: {}, 大小: {} 字节", tempFile.getAbsolutePath(), tempFile.length());

                dbReader = new DatabaseReader.Builder(tempFile).build();
                dbLoaded = true;
                logger.info("GeoLite2数据库加载成功 (类路径)");
                return;
            }

            // 从绝对路径加载
            String absolutePath = "D:\\bigdata\\strea-bda-prod\\stream-realtime\\src\\main\\resources\\GeoLite2-City.mmdb";
            logger.info("尝试从绝对路径加载: {}", absolutePath);

            File databaseFile = new File(absolutePath);
            if (databaseFile.exists()) {
                logger.info("文件存在，大小: {} 字节", databaseFile.length());
                dbReader = new DatabaseReader.Builder(databaseFile).build();
                dbLoaded = true;
                logger.info("GeoLite2数据库加载成功 (绝对路径)");
                return;
            }

            logger.error("所有加载方法都失败！请确认GeoLite2-City.mmdb文件位置");
            logger.error("1. src/main/resources/GeoLite2-City.mmdb");
            logger.error("2. {}", absolutePath);

        } catch (Exception e) {
            logger.error("IP数据库初始化失败", e);
        }
    }

    /**
     * 获取详细的位置信息 [省份, 城市, 完整位置]
     */
    @SneakyThrows
    public static String[] getDetailedLocation(String ip) {
        String[] result = {"未知", "未知", "未知"};

        if (!isValidIP(ip)) {
            return result;
        }

        // 检查内网IP
        if (isInternalIP(ip)) {
            return new String[]{"内网", "内网", "内网"};
        }

        String cacheKey = ip + "_detailed";
        if (LOCATION_CACHE.containsKey(cacheKey)) {
            return LOCATION_CACHE.get(cacheKey).split("\\|", 3);
        }

        if (!dbLoaded || dbReader == null) {
            logger.warn("IP数据库未加载，无法查询: {}", ip);
            return new String[]{"数据库未加载", "数据库未加载", "数据库未加载"};
        }

        try {
            InetAddress ipAddress = InetAddress.getByName(ip);
            CityResponse response = dbReader.city(ipAddress);

            Country country = response.getCountry();
            Subdivision subdivision = response.getMostSpecificSubdivision();
            City city = response.getCity();

            String countryName = getCountryName(country);
            String province = getProvinceName(subdivision);
            String cityName = getCityName(city);

            // 检查是否为国外地址
            if (!isChineseLocation(countryName)) {
                logger.info("检测到国外地址: {} -> {}", ip, countryName);
                return new String[]{"国外", "国外", "国外"};
            }

            // 标准化省份名称
            province = standardizeProvinceName(province);

            // 如果城市未知但省份已知，使用省份作为城市
            if ("未知".equals(cityName) && !"未知".equals(province)) {
                cityName = province;
            }

            // 构建完整位置字符串
            String fullLocation = buildFullLocation(province, cityName);

            result[0] = province;
            result[1] = cityName;
            result[2] = fullLocation;

            // 放入缓存
            LOCATION_CACHE.put(cacheKey, province + "|" + cityName + "|" + fullLocation);

            logger.debug("IP详细位置查询: {} -> {}/{}", ip, province, cityName);

            return result;

        } catch (Exception e) {
            logger.error("IP详细位置查询失败: {}, 错误: {}", ip, e.getMessage());
            return result;
        }
    }

    /**
     * 获取包含ISP的四级位置信息 [省份, 城市, 完整位置, 运营商]
     */
    @SneakyThrows
    public static String[] getDetailedLocationWithISP(String ip) {
        String[] result = {"未知", "未知", "未知", "未知"};

        if (!isValidIP(ip)) {
            return result;
        }

        // 检查内网IP
        if (isInternalIP(ip)) {
            return new String[]{"内网", "内网", "内网", "内网"};
        }

        String cacheKey = ip + "_detailed_isp";
        if (LOCATION_CACHE.containsKey(cacheKey)) {
            return LOCATION_CACHE.get(cacheKey).split("\\|", 4);
        }

        // 使用原有的地理位置查询
        String[] location = getDetailedLocation(ip);
        String province = location[0];
        String city = location[1];
        String fullLocation = location[2];

        // 如果是国外地址，直接返回
        if ("国外".equals(province)) {
            return new String[]{"国外", "国外", "国外", "国外"};
        }

        // 检测运营商
        String isp = detectISP(ip);

        result[0] = province;
        result[1] = city;
        result[2] = fullLocation;
        result[3] = isp;

        // 放入缓存
        LOCATION_CACHE.put(cacheKey, province + "|" + city + "|" + fullLocation + "|" + isp);

        logger.debug("IP位置+运营商: {} -> {}/{}/{}", ip, province, city, isp);

        return result;
    }

    /**
     * 获取简单位置信息
     */
    @SneakyThrows
    public static String getLocation(String ip) {
        if (!isValidIP(ip)) {
            return "未知";
        }

        // 检查内网IP
        if (isInternalIP(ip)) {
            return "内网";
        }

        // 检查缓存
        if (LOCATION_CACHE.containsKey(ip)) {
            return LOCATION_CACHE.get(ip);
        }

        if (!dbLoaded || dbReader == null) {
            logger.warn("IP数据库未加载，无法查询: {}", ip);
            return "数据库未加载";
        }

        try {
            InetAddress ipAddress = InetAddress.getByName(ip);
            CityResponse response = dbReader.city(ipAddress);

            Country country = response.getCountry();
            Subdivision subdivision = response.getMostSpecificSubdivision();
            City city = response.getCity();

            // 检查是否为国外地址
            String countryName = getCountryName(country);
            if (!isChineseLocation(countryName)) {
                return "国外";
            }

            String province = getProvinceName(subdivision);
            String cityName = getCityName(city);

            // 标准化省份名称
            province = standardizeProvinceName(province);

            String result = buildFullLocation(province, cityName);

            // 放入缓存
            LOCATION_CACHE.put(ip, result);

            logger.debug("IP查询成功: {} -> {}", ip, result);

            return result;

        } catch (Exception e) {
            logger.error("IP查询失败: {}, 错误: {}", ip, e.getMessage());
            return "未知位置";
        }
    }

    // ========== 辅助方法 ==========

    /**
     * 验证IP地址是否有效
     */
    private static boolean isValidIP(String ip) {
        return ip != null && !ip.isEmpty() && !"unknown".equalsIgnoreCase(ip);
    }

    /**
     * 获取国家名称
     */
    private static String getCountryName(Country country) {
        if (country == null) return "未知";

        String countryName = country.getNames().get("zh-CN");
        if (countryName == null) {
            String englishName = country.getName();
            countryName = translateToChinese(englishName, COUNTRY_TRANSLATION_MAP);
        }
        return countryName != null ? countryName : "未知";
    }

    /**
     * 获取省份名称
     */
    private static String getProvinceName(Subdivision subdivision) {
        if (subdivision == null) return "未知";

        String province = subdivision.getNames().get("zh-CN");
        if (province == null) {
            String englishName = subdivision.getName();
            province = translateToChinese(englishName, PROVINCE_TRANSLATION_MAP);
        }
        return province != null ? province : "未知";
    }

    /**
     * 获取城市名称
     */
    private static String getCityName(City city) {
        if (city == null) return "未知";

        String cityName = city.getNames().get("zh-CN");
        if (cityName == null) {
            String englishName = city.getName();
            cityName = translateToChinese(englishName, CITY_TRANSLATION_MAP);
        }
        return cityName != null ? cityName : "未知";
    }

    /**
     * 构建完整位置字符串
     */
    private static String buildFullLocation(String province, String city) {
        if (!"未知".equals(province) && !"未知".equals(city)) {
            return province + "-" + city;
        } else if (!"未知".equals(province)) {
            return province;
        } else if (!"未知".equals(city)) {
            return city;
        } else {
            return "未知位置";
        }
    }

    /**
     * 检查是否为国内地址
     */
    private static boolean isChineseLocation(String countryName) {
        if (countryName == null) return false;

        String lowerCountry = countryName.toLowerCase();
        return lowerCountry.contains("中国") ||
                lowerCountry.contains("china") ||
                lowerCountry.equals("cn") ||
                lowerCountry.contains("香港") || lowerCountry.contains("hong kong") ||
                lowerCountry.contains("澳门") || lowerCountry.contains("macao") || lowerCountry.contains("macau") ||
                lowerCountry.contains("台湾") || lowerCountry.contains("taiwan");
    }

    /**
     * 翻译英文名称为中文
     */
    private static String translateToChinese(String englishName, Map<String, String> translationMap) {
        if (englishName == null) return null;

        String lowerCaseName = englishName.toLowerCase();

        // 直接匹配
        if (translationMap.containsKey(lowerCaseName)) {
            return translationMap.get(lowerCaseName);
        }

        // 尝试部分匹配
        for (Map.Entry<String, String> entry : translationMap.entrySet()) {
            if (lowerCaseName.contains(entry.getKey())) {
                return entry.getValue();
            }
        }

        // 如果无法翻译且包含英文字符，标记为国外
        if (lowerCaseName.matches(".*[a-zA-Z].*")) {
            logger.info("检测到英文地址，标记为国外: {}", englishName);
            return "国外";
        }

        return englishName;
    }

    /**
     * 运营商检测方法 - 修复版（解决运营商重叠问题）
     */
    private static String detectISP(String ip) {
        if (ip == null || ip.isEmpty()) return "未知运营商";

        // 检查内网IP
        if (isInternalIP(ip)) {
            return "内网";
        }

        String[] ipParts = ip.split("\\.");
        if (ipParts.length < 2) return "未知运营商";

        String firstSegment = ipParts[0];
        String secondSegment = ipParts[1];

        // 添加调试日志
        System.out.println("🔍 IP运营商检测: " + ip + " (first=" + firstSegment + ", second=" + secondSegment + ")");

        // 更精确的运营商IP段分配（避免重叠）

        // 中国移动 - 优先级最高
        if (isMobileIP(firstSegment, secondSegment)) {
            System.out.println("✅ 识别为中国移动: " + ip);
            return "中国移动";
        }

        // 中国联通 - 优先级第二
        if (isUnicomIP(firstSegment, secondSegment)) {
            System.out.println("✅ 识别为中国联通: " + ip);
            return "中国联通";
        }

        // 中国电信 - 优先级第三
        if (isTelecomIP(firstSegment, secondSegment)) {
            System.out.println("✅ 识别为中国电信: " + ip);
            return "中国电信";
        }

        System.out.println("❓ 识别为其他运营商: " + ip);
        return "其他运营商";
    }

    /**
     * 判断是否为中国移动IP
     */
    private static boolean isMobileIP(String firstSegment, String secondSegment) {
        // 中国移动主要IP段（无重叠的独有段）
        String[] mobileSegments = {
                "39", "47", "111", "112", "114", "115", "117",
                "120", "121", "122", "123", "124", "183", "223"
        };

        for (String segment : mobileSegments) {
            if (segment.equals(firstSegment)) {
                return true;
            }
        }

        // 移动特定IP段
        if ("117".equals(firstSegment) && isSegmentInRange(secondSegment, 128, 191)) {
            return true;
        }

        // 113段 - 移动和电信重叠，但移动占主要部分
        if ("113".equals(firstSegment)) {
            return isSegmentInRange(secondSegment, 0, 127); // 移动113.0-113.127
        }

        return false;
    }

    /**
     * 判断是否为中国联通IP
     */
    private static boolean isUnicomIP(String firstSegment, String secondSegment) {
        // 中国联通主要IP段（无重叠的独有段）
        String[] unicomSegments = {
                "42", "43", "58", "59", "60", "61", "116", "118", "119",
                "122", "123", "124", "125", "171", "175", "182",
                "202", "210", "211", "218", "219", "220"
        };

        for (String segment : unicomSegments) {
            if (segment.equals(firstSegment)) {
                // 对于重叠段，需要更精确的判断
                if ("58".equals(firstSegment)) {
                    return isSegmentInRange(secondSegment, 16, 31); // 联通58.16-58.31
                }
                if ("59".equals(firstSegment)) {
                    return isSegmentInRange(secondSegment, 32, 63); // 联通59.32-59.63
                }
                if ("60".equals(firstSegment)) {
                    return isSegmentInRange(secondSegment, 0, 255); // 联通60.x
                }
                if ("61".equals(firstSegment)) {
                    return isSegmentInRange(secondSegment, 128, 191); // 联通61.128-61.191
                }
                return true;
            }
        }

        // 联通特定IP段
        if ("110".equals(firstSegment)) {
            return isSegmentInRange(secondSegment, 0, 63); // 联通110.0-110.63
        }

        // 113段 - 联通部分
        if ("113".equals(firstSegment)) {
            return isSegmentInRange(secondSegment, 192, 255); // 联通113.192-113.255
        }

        return false;
    }

    /**
     * 判断是否为中国电信IP
     */
    private static boolean isTelecomIP(String firstSegment, String secondSegment) {
        // 中国电信主要IP段
        String[] telecomSegments = {
                "14", "27", "36", "49", "58", "59", "60", "61", "106", "110", "111", "112",
                "113", "114", "115", "116", "117", "118", "119", "120", "121", "122", "123",
                "124", "125", "126", "171", "175", "180", "182", "183", "202", "203", "210",
                "211", "218", "219", "220", "221", "222"
        };

        for (String segment : telecomSegments) {
            if (segment.equals(firstSegment)) {
                // 对于重叠段，需要更精确的判断
                if ("58".equals(firstSegment)) {
                    return !isSegmentInRange(secondSegment, 16, 31); // 电信排除58.16-58.31（这部分是联通）
                }
                if ("59".equals(firstSegment)) {
                    return !isSegmentInRange(secondSegment, 32, 63); // 电信排除59.32-59.63（这部分是联通）
                }
                if ("60".equals(firstSegment)) {
                    return isSegmentInRange(secondSegment, 0, 255); // 电信60.x
                }
                if ("61".equals(firstSegment)) {
                    return !isSegmentInRange(secondSegment, 128, 191); // 电信排除61.128-61.191（这部分是联通）
                }
                if ("113".equals(firstSegment)) {
                    return isSegmentInRange(secondSegment, 128, 191); // 电信113.128-113.191
                }
                return true;
            }
        }

        // 电信特定IP段
        if ("106".equals(firstSegment)) {
            return isSegmentInRange(secondSegment, 0, 127); // 电信106.0-106.127
        }

        return false;
    }

    /**
     * 检查IP段是否在指定范围内（重命名避免冲突）
     */
    private static boolean isSegmentInRange(String segment, int start, int end) {
        try {
            int seg = Integer.parseInt(segment);
            return seg >= start && seg <= end;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * 标准化省份名称
     */
    private static String standardizeProvinceName(String province) {
        if (province == null) return "未知";

        // 移除后缀
        province = province.replace("省", "")
                .replace("市", "")
                .replace("壮族自治区", "")
                .replace("自治区", "")
                .replace("回族自治区", "")
                .replace("维吾尔自治区", "");

        // 特殊处理
        if (province.contains("广西")) return "广西";
        if (province.contains("内蒙古")) return "内蒙古";
        if (province.contains("新疆")) return "新疆";
        if (province.contains("宁夏")) return "宁夏";
        if (province.contains("西藏")) return "西藏";

        return province;
    }

    /**
     * 检查是否为内网IP
     */
    private static boolean isInternalIP(String ip) {
        if (ip == null) return false;
        return ip.startsWith("10.") ||
                ip.startsWith("192.168.") ||
                (ip.startsWith("172.") && isIPInRange(ip, 16, 31)) ||
                ip.equals("127.0.0.1");
    }

    /**
     * 检查IP是否在指定范围内（重命名避免冲突）
     */
    private static boolean isIPInRange(String ip, int start, int end) {
        try {
            String[] parts = ip.split("\\.");
            if (parts.length >= 2) {
                int second = Integer.parseInt(parts[1]);
                return second >= start && second <= end;
            }
        } catch (Exception e) {
            // 忽略解析错误
        }
        return false;
    }

    public static String generateHeatMapReport(Map<String, Map<String, Long>> provinceCityMap, long totalVisits) {
        StringBuilder report = new StringBuilder();
        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
        String currentDate = sdf.format(new Date());

        // 计算覆盖的省份和城市数量
        Set<String> provinces = provinceCityMap.keySet();
        Set<String> cities = new HashSet<>();
        for (Map<String, Long> cityMap : provinceCityMap.values()) {
            cities.addAll(cityMap.keySet());
        }

        report.append("🌍 全国省份城市热力图\n");
        report.append("🌍 ").append(currentDate).append(" 全国访问热力图\n");
        report.append("=================================================\n");
        report.append(String.format("总访问量: %d | 覆盖省份: %d | 覆盖城市: %d\n",
                totalVisits, provinces.size(), cities.size()));
        report.append("=================================================\n");
        report.append("省份           城市           访问量        占比      \n");
        report.append("-------------------------------------------------\n");

        // 收集所有城市数据用于排序
        List<Object[]> allCityData = new ArrayList<>(); // 使用Object数组 [province, city, visitCount]

        for (Map.Entry<String, Map<String, Long>> provinceEntry : provinceCityMap.entrySet()) {
            String province = provinceEntry.getKey();
            Map<String, Long> cityMap = provinceEntry.getValue();

            for (Map.Entry<String, Long> cityEntry : cityMap.entrySet()) {
                allCityData.add(new Object[]{province, cityEntry.getKey(), cityEntry.getValue()});
            }
        }

        // 按访问量降序排序所有城市
        allCityData.sort((a, b) -> Long.compare((Long) b[2], (Long) a[2]));

        // 输出所有城市数据
        for (Object[] cityData : allCityData) {
            String province = (String) cityData[0];
            String city = (String) cityData[1];
            long count = (Long) cityData[2];
            double percentage = (count * 100.0) / totalVisits;

            // 格式化输出
            String provinceDisplay = String.format("%-12s",
                    province.length() > 12 ? province.substring(0, 12) : province);
            String cityDisplay = String.format("%-12s",
                    city.length() > 12 ? city.substring(0, 12) : city);

            String percentageStr = String.format("%.1f%%", percentage);
            String countStr = String.format("%-12d", count);
            String percentageDisplay = String.format("%-10s", percentageStr);

            // 添加热度图标
            String heatIcon = getHeatIcon(percentage);

            report.append(String.format("%s%s%s%s%s\n",
                    provinceDisplay, cityDisplay, countStr, percentageDisplay, heatIcon));
        }

        return report.toString();
    }

    /**
     * 热度图标方法
     */
    public static String getHeatIcon(double percentage) {
        if (percentage >= 10.0) return " 🔥";
        if (percentage >= 5.0) return " ⚡";
        if (percentage >= 3.0) return " 🔶";
        if (percentage >= 1.0) return " 🔸";
        return "   ";
    }

    /**
     * 清空缓存
     */
    public static void clearCache() {
        LOCATION_CACHE.clear();
    }

    /**
     * 检查数据库是否加载
     */
    public static boolean isDatabaseLoaded() {
        return dbLoaded;
    }
}