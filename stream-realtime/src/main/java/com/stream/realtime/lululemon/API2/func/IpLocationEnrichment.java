package com.stream.realtime.lululemon.API2.func;

import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class IpLocationEnrichment extends RichMapFunction<JsonObject, JsonObject> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("=== IP地址转换处理器初始化 (GeoLite2) ===");

        // 等待数据库加载完成
        Thread.sleep(1000);

        if (IPLocationUtils.isDatabaseLoaded()) {
            System.out.println("✅ GeoLite2数据库已成功加载");
        } else {
            System.err.println("❌ GeoLite2数据库加载失败！");
            System.err.println("请检查:");
            System.err.println("1. 文件位置: src/main/resources/GeoLite2-City.mmdb");
            System.err.println("2. 文件大小: 应该大于50MB");
            System.err.println("3. Maven依赖: com.maxmind.geoip2:geoip2:2.15.0");
        }
    }

    @Override
    public JsonObject map(JsonObject jsonObject) throws Exception {
        try {
            // 复制原始JSON对象，避免修改原对象
            JsonObject enrichedJson = jsonObject.deepCopy();

            // 检查是否存在gis字段和ip字段
            if (enrichedJson.has("gis") && enrichedJson.get("gis").isJsonObject()) {
                JsonObject gisObject = enrichedJson.getAsJsonObject("gis");

                if (gisObject.has("ip") && !gisObject.get("ip").isJsonNull()) {
                    String ip = gisObject.get("ip").getAsString();

                    // 查询IP地理位置
                    String location = IPLocationUtils.getLocation(ip);

                    // 添加location字段到gis对象
                    gisObject.addProperty("location", location);

                    // 单独添加地区字段到根级别
                    enrichedJson.addProperty("ip_location", location);
                }
            }

            return enrichedJson;

        } catch (Exception e) {
            System.err.println("IP地址转换处理失败: " + e.getMessage());
            // 处理失败时返回原始数据
            return jsonObject;
        }
    }
}