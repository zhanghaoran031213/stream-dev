package com.stream.realtime.lululemon.API1;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import utils.EnvironmentSettingUtils;

import java.time.ZoneId;

/**
 * @Author: ZHR
 * @Date: 2025/10/27 14:58
 * @Description:
 **/
public class FlinkTestSQL {

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
                "    is_online_sales string,\n" +
                "    shipping_address string,\n" +
                "    recommendations_product_ids string,\n" +
                "    ds string,\n" +
                "    ts bigint,\n" +
                "    ts_ms as case when ts < 100000000000 then to_timestamp_ltz(ts * 1000, 3) else to_timestamp_ltz(ts, 3) end,\n" +
                "    insert_time string,\n" +
                "    table_name string,\n" +
                "    op string,\n" +
                "    watermark for ts_ms as ts_ms - interval '5' second\n" +
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

//        tenv.executeSql(
//                "SELECT DATE_FORMAT(window_start, 'yyyy-MM-dd') as order_date, \n" +
//                        "    window_start, \n" +
//                        "    window_end, \n" +
//                        "    SUM(CAST(total_amount AS DECIMAL(18,2))) as GMV\n" +
//                        "FROM TABLE(\n" +
//                        "    CUMULATE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' MINUTES,INTERVAL '1' DAY)\n" +
//                        ")\n" +
//                        "WHERE DATE_FORMAT(ts_ms, 'yyyy-MM-dd') = '2025-10-27'\n" +
//                        "GROUP BY window_start, window_end"
//        ).print();




//        tenv.executeSql(
//                "WITH window_gmv AS (\n" +
//                        "    SELECT \n" +
//                        "        window_start,\n" +
//                        "        window_end,\n" +
//                        "        SUM(CAST(total_amount AS DECIMAL(18,2))) as total_gmv\n" +
//                        "    FROM TABLE(\n" +
//                        "        CUMULATE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' MINUTES, INTERVAL '1' DAY)\n" +
//                        "    )\n" +
//                        "    WHERE DATE_FORMAT(ts_ms, 'yyyy-MM-dd') = '2025-10-27'\n" +
//                        "    GROUP BY window_start, window_end\n" +
//                        "),\n" +
//                        "top_products AS (\n" +
//                        "    SELECT \n" +
//                        "        window_start,\n" +
//                        "        window_end,\n" +
//                        "        LISTAGG(id, ',') as top5_ids\n" +
//                        "    FROM (\n" +
//                        "        SELECT \n" +
//                        "            window_start,\n" +
//                        "            window_end,\n" +
//                        "            id,\n" +
//                        "            ROW_NUMBER() OVER (\n" +
//                        "                PARTITION BY window_start, window_end \n" +
//                        "                ORDER BY SUM(CAST(total_amount AS DECIMAL(18,2))) DESC\n" +
//                        "            ) as rn\n" +
//                        "        FROM TABLE(\n" +
//                        "            CUMULATE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' MINUTES, INTERVAL '1' DAY)\n" +
//                        "        )\n" +
//                        "        WHERE DATE_FORMAT(ts_ms, 'yyyy-MM-dd') = '2025-10-27'\n" +
//                        "        GROUP BY window_start, window_end, id\n" +
//                        "    )\n" +
//                        "    WHERE rn <= 5\n" +
//                        "    GROUP BY window_start, window_end\n" +
//                        ")\n" +
//                        "SELECT \n" +
//                        "    DATE_FORMAT(wg.window_start, 'yyyy-MM-dd') as order_date, \n" +
//                        "    wg.window_start, \n" +
//                        "    wg.window_end, \n" +
//                        "    wg.total_gmv as GMV,\n" +
//                        "    COALESCE(tp.top5_ids, '') as top5_ids\n" +
//                        "FROM window_gmv wg\n" +
//                        "LEFT JOIN top_products tp ON wg.window_start = tp.window_start AND wg.window_end = tp.window_end"
//        ).print();


        tenv.executeSql(
                "WITH window_gmv AS (\n" +
                        "    SELECT \n" +
                        "        window_start,\n" +
                        "        window_end,\n" +
                        "        SUM(CAST(total_amount AS DECIMAL(18,2))) as total_gmv\n" +
                        "    FROM TABLE(\n" +
                        "        CUMULATE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' MINUTES, INTERVAL '1' DAY)\n" +
                        "    )\n" +
                        "    WHERE DATE_FORMAT(ts_ms, 'yyyy-MM-dd') = '2025-10-27'\n" +
                        "    GROUP BY window_start, window_end\n" +
                        "),\n" +
                        "top_ids AS (\n" +
                        "    SELECT \n" +
                        "        window_start,\n" +
                        "        window_end,\n" +
                        "        LISTAGG(id, ',') as top5_ids\n" +
                        "    FROM (\n" +
                        "        SELECT \n" +
                        "            window_start,\n" +
                        "            window_end,\n" +
                        "            id,\n" +
                        "            ROW_NUMBER() OVER (\n" +
                        "                PARTITION BY window_start, window_end \n" +
                        "                ORDER BY SUM(CAST(total_amount AS DECIMAL(18,2))) DESC\n" +
                        "            ) as rn\n" +
                        "        FROM TABLE(\n" +
                        "            CUMULATE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' MINUTES, INTERVAL '1' DAY)\n" +
                        "        )\n" +
                        "        WHERE DATE_FORMAT(ts_ms, 'yyyy-MM-dd') = '2025-10-27'\n" +
                        "        GROUP BY window_start, window_end, id\n" +
                        "    )\n" +
                        "    WHERE rn <= 5\n" +
                        "    GROUP BY window_start, window_end\n" +
                        "),\n" +
                        "top_products AS (\n" +
                        "    SELECT \n" +
                        "        window_start,\n" +
                        "        window_end,\n" +
                        "        LISTAGG(product_id, ',') as top5_product_ids\n" +
                        "    FROM (\n" +
                        "        SELECT \n" +
                        "            window_start,\n" +
                        "            window_end,\n" +
                        "            product_id,\n" +
                        "            ROW_NUMBER() OVER (\n" +
                        "                PARTITION BY window_start, window_end \n" +
                        "                ORDER BY SUM(CAST(total_amount AS DECIMAL(18,2))) DESC\n" +
                        "            ) as rn\n" +
                        "        FROM TABLE(\n" +
                        "            CUMULATE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' MINUTES, INTERVAL '1' DAY)\n" +
                        "        )\n" +
                        "        WHERE DATE_FORMAT(ts_ms, 'yyyy-MM-dd') = '2025-10-27'\n" +
                        "        GROUP BY window_start, window_end, product_id\n" +
                        "    )\n" +
                        "    WHERE rn <= 5\n" +
                        "    GROUP BY window_start, window_end\n" +
                        ")\n" +
                        "SELECT \n" +
                        "    DATE_FORMAT(wg.window_start, 'yyyy-MM-dd') as order_date, \n" +
                        "    wg.window_start, \n" +
                        "    wg.window_end, \n" +
                        "    wg.total_gmv as GMV,\n" +
                        "    COALESCE(ti.top5_ids, '') as top5_ids,\n" +
                        "    COALESCE(tp.top5_product_ids, '') as top5_product_ids\n" +
                        "FROM window_gmv wg\n" +
                        "LEFT JOIN top_ids ti ON wg.window_start = ti.window_start AND wg.window_end = ti.window_end\n" +
                        "LEFT JOIN top_products tp ON wg.window_start = tp.window_start AND wg.window_end = tp.window_end"
        ).print();

        env.execute();

    }

}
