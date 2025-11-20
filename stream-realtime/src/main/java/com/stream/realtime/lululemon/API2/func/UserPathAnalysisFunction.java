package com.stream.realtime.lululemon.API2.func;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

/**
 * @Author: ZHR
 * @Date: 2025/11/2 17:49
 * @Description: 用户路径分析函数 - 只保留业务页面
 **/
public class UserPathAnalysisFunction extends KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, String>> {

    private transient ValueState<List<String>> userPathState;
    private transient ValueState<Long> lastTimestampState;
    private transient ValueState<String> userDateState;
    private static final long SESSION_TIMEOUT = 2 * 60 * 60 * 1000; // 延长到2小时会话超时

    // 定义允许的业务页面集合
    private static final Set<String> BUSINESS_PAGES = new HashSet<String>() {{
        add("search");
        add("home");
        add("product_list");
        add("login");
        add("product_detail");
        add("payment");
    }};

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<List<String>> pathDescriptor =
                new ValueStateDescriptor<>(
                        "userPathState",
                        TypeInformation.of(new TypeHint<List<String>>() {})
                );
        userPathState = getRuntimeContext().getState(pathDescriptor);

        ValueStateDescriptor<Long> timestampDescriptor =
                new ValueStateDescriptor<>("lastTimestampState", TypeInformation.of(Long.class));
        lastTimestampState = getRuntimeContext().getState(timestampDescriptor);

        ValueStateDescriptor<String> dateDescriptor =
                new ValueStateDescriptor<>("userDateState", TypeInformation.of(String.class));
        userDateState = getRuntimeContext().getState(dateDescriptor);
    }

    @Override
    public void processElement(
            Tuple2<String, String> value,
            Context ctx,
            Collector<Tuple2<String, String>> out) throws Exception {

        List<String> currentPath = userPathState.value();
        Long lastTimestamp = lastTimestampState.value();

        // 解析key：userId|date
        String[] keyParts = value.f0.split("\\|");
        String userId = keyParts[0];
        String currentDate = keyParts.length > 1 ? keyParts[1] : "unknown";

        // 解析value：logType|timestamp
        String[] valueParts = value.f1.split("\\|");
        String logType = valueParts[0];
        Long currentTimestamp = Long.parseLong(valueParts[1]);

        // 检查是否新会话（超过2小时无活动）或者日期变化
        String lastDate = userDateState.value();
        if (lastTimestamp == null || currentTimestamp - lastTimestamp > SESSION_TIMEOUT || !currentDate.equals(lastDate)) {
            currentPath = new ArrayList<>();
            userDateState.update(currentDate);
        } else if (currentPath == null) {
            currentPath = new ArrayList<>();
        }

        // 只处理业务页面，过滤技术性操作
        if (BUSINESS_PAGES.contains(logType)) {
            // 过滤连续重复的页面（避免 search -> search -> search）
            if (currentPath.isEmpty() || !currentPath.get(currentPath.size() - 1).equals(logType)) {
                currentPath.add(logType);
            }
        }

        // 限制路径长度，避免内存溢出
        if (currentPath.size() > 15) {
            currentPath.remove(0);
        }

        userPathState.update(currentPath);
        lastTimestampState.update(currentTimestamp);

        // 输出业务路径，只包含业务页面
        if (currentPath.size() >= 2) { // 只输出至少2个页面的路径
            String pathSequence = String.join(" -> ", currentPath);

            // 调试输出多页面路径
            System.out.println("Generated Business Path - User: " + userId +
                    ", Date: " + currentDate +
                    ", Path: " + pathSequence +
                    ", Length: " + currentPath.size());

            // 在输出中包含日期信息
            out.collect(Tuple2.of(currentDate, pathSequence));
        }
    }
}