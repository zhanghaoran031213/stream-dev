package com.stream.realtime.lululemon.API1;

//import com.stream.common.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import utils.EnvironmentSettingUtils;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TestFinkSqlDoris2 {
    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);

        EnvironmentSettingUtils.defaultParameter(env);
        env.setStateBackend(new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                new org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage());

//        // 然后覆盖 RocksDB 配置
//        env.setStateBackend(new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(
//                new org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage());


        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        tenv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

        String source_kafka_order_info_ddl = "create table if not exists t_kafka_oms_order_info (\n" +
                "    id string,\n" +
                "    order_id string,\n" +
                "    user_id string,\n" +
                "    user_name string,\n" +
                "    phone_number string,\n" +
                "    product_link string,\n" +
                "    product_id string,\n" +
                "    color string,\n" +
                "    size string,\n" +
                "    item_id string,\n" +
                "    material string,\n" +
                "    sale_num string,\n" +
                "    sale_amount string,\n" +
                "    total_amount string,\n" +
                "    product_name string,\n" +
                "    ts_ms as case when ts < 100000000000 then to_timestamp_ltz(ts * 1000, 3) else to_timestamp_ltz(ts, 3) end,\n" +
                "    is_online_sales string,\n" +
                "    shipping_address string,\n" +
                "    recommendations_product_ids string,\n" +
                "    ds string,\n" +
                "    ts bigint,\n" +
                "    insert_time string,\n" +
                "    table_name string,\n" +
                "    op string,\n" +
                "    watermark for ts_ms as ts_ms - interval '1' second\n" +
                ")\n" +
                "with (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'realtime_v3_order_info',\n" +
                "    'properties.bootstrap.servers'= 'cdh01:9092,cdh02:9092,cdh03:9092',\n" +
                "    'properties.group.id' = 'order-analysis1',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ")";

        tenv.executeSql(source_kafka_order_info_ddl);



        String dorisSinkDDL = String.format(
                "CREATE TABLE IF NOT EXISTS order_window_summary (\n" +
                        "    ds DATE,\n" +
                        "    window_start STRING,\n" +
                        "    window_end STRING,\n" +
                        "    gmv DECIMAL(18,2),\n" +
                        "    top5_ids STRING,\n" +
                        "    top5_product_ids STRING,\n" +
                        "    PRIMARY KEY (ds, window_start, window_end) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "    'connector' = 'doris',\n" +
                        "    'fenodes' = '192.168.200.32:8030', \n" +
                        "    'table.identifier' = 'sprider_db.doris_gmv_top_products', \n" +
                        "    'username' = 'root', \n" +
                        "    'password' = 'zhr123,./',     \n" +
                        "    'sink.label-prefix' = 'lululemon_gmv_%d',\n" +
                        "    'sink.enable-2pc' = 'true',\n" +
                        "    'sink.properties.format' = 'json',\n" +
                        "    'sink.properties.read_json_by_line' = 'true'\n" +
                        ")", System.currentTimeMillis());


        tenv.executeSql(dorisSinkDDL);

        String compute0ToCurrentDaySaleAmountGMV =
                "WITH base_t AS ( " +
                        "    SELECT " +
                        "        id, " +  // 添加 id 字段
                        "        TRIM(REPLACE(product_id, '\\n', '')) AS clean_product_id, " +
                        "        CAST(total_amount AS DECIMAL(18,2)) AS total_amount, " +
                        "        ts_ms " +
                        "    FROM t_kafka_oms_order_info " +
                        "), " +
                        "window_product_gmv AS ( " +
                        "    SELECT " +
                        "        window_start, " +
                        "        window_end, " +
                        "        id, " +  // 添加 id 字段
                        "        clean_product_id, " +
                        "        SUM(total_amount) AS product_gmv " +
                        "    FROM TABLE( " +
                        "        CUMULATE(TABLE base_t, DESCRIPTOR(ts_ms), INTERVAL '10' MINUTES, INTERVAL '1' DAY) " +
                        "    ) " +
                        "    GROUP BY window_start, window_end, id, clean_product_id " +  // 添加 id 到分组
                        "), " +
                        "ranked_orders AS ( " +  // 改为 ranked_orders，按订单ID排名
                        "    SELECT " +
                        "        window_start, " +
                        "        window_end, " +
                        "        id, " +
                        "        product_gmv, " +
                        "        ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY product_gmv DESC) AS rn " +
                        "    FROM window_product_gmv " +
                        "), " +
                        "top5_ids AS ( " +
                        "    SELECT " +
                        "        window_start, " +
                        "        window_end, " +
                        "        LISTAGG(id, ',') AS top5_ids " +  // 使用 id 而不是 product_id
                        "    FROM ranked_orders " +
                        "    WHERE rn <= 5 " +
                        "    GROUP BY window_start, window_end " +
                        "), " +
                        "ranked_products AS ( " +  // 单独处理产品排名
                        "    SELECT " +
                        "        window_start, " +
                        "        window_end, " +
                        "        clean_product_id, " +
                        "        SUM(product_gmv) AS total_product_gmv, " +  // 按产品汇总GMV
                        "        ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY SUM(product_gmv) DESC) AS rn " +
                        "    FROM window_product_gmv " +
                        "    GROUP BY window_start, window_end, clean_product_id " +
                        "), " +
                        "top5_products AS ( " +
                        "    SELECT " +
                        "        window_start, " +
                        "        window_end, " +
                        "        LISTAGG(clean_product_id, ',') AS top5_product_ids " +
                        "    FROM ranked_products " +
                        "    WHERE rn <= 5 " +
                        "    GROUP BY window_start, window_end " +
                        "), " +
                        "win_agg AS ( " +
                        "    SELECT " +
                        "        window_start, " +
                        "        window_end, " +
                        "        CAST(DATE_FORMAT(window_start, 'yyyy-MM-dd') AS DATE) AS ds, " +
                        "        SUM(product_gmv) AS gmv " +
                        "    FROM window_product_gmv " +
                        "    GROUP BY window_start, window_end " +
                        ") " +
                        "SELECT " +
                        "    w.ds, " +
                        "    DATE_FORMAT(w.window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start, " +
                        "    DATE_FORMAT(w.window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end, " +
                        "    CAST(w.gmv AS DECIMAL(18,2)) AS gmv, " +
                        "    COALESCE(ti.top5_ids, '') AS top5_ids, " +  // 订单ID的top5
                        "    COALESCE(tp.top5_product_ids, '') AS top5_product_ids " +  // 产品ID的top5
                        "FROM win_agg w " +
                        "LEFT JOIN top5_ids ti " +  // 连接订单ID的top5
                        "    ON w.window_start = ti.window_start " +
                        "    AND w.window_end = ti.window_end " +
                        "LEFT JOIN top5_products tp " +  // 连接产品ID的top5
                        "    ON w.window_start = tp.window_start " +
                        "    AND w.window_end = tp.window_end";


        tenv.executeSql("insert into order_window_summary " + compute0ToCurrentDaySaleAmountGMV);

        tenv.executeSql(compute0ToCurrentDaySaleAmountGMV).print();

    }
}