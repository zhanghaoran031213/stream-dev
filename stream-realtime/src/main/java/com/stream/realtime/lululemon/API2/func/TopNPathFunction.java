package com.stream.realtime.lululemon.API2.func;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

/**
 * @Author: ZHR
 * @Date: 2025/11/2 18:02
 * @Description: 优化版TOP N热门路径函数，避免重复输出
 **/
public class TopNPathFunction extends KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>> {

    private final int topN;
    private transient ValueState<Map<String, Long>> pathCountsState;
    private transient ValueState<String> lastOutputState; // 记录上次输出内容，避免重复

    public TopNPathFunction(int topN) {
        this.topN = topN;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Map<String, Long>> pathCountsDescriptor =
                new ValueStateDescriptor<>("pathCountsState",
                        TypeInformation.of(new TypeHint<Map<String, Long>>() {}));
        pathCountsState = getRuntimeContext().getState(pathCountsDescriptor);

        ValueStateDescriptor<String> lastOutputDescriptor =
                new ValueStateDescriptor<>("lastOutputState", TypeInformation.of(String.class));
        lastOutputState = getRuntimeContext().getState(lastOutputDescriptor);
    }

    @Override
    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
        Map<String, Long> pathCounts = pathCountsState.value();
        if (pathCounts == null) {
            pathCounts = new HashMap<>();
        }

        // 更新路径计数
        pathCounts.put(value.f0, value.f1);
        pathCountsState.update(pathCounts);

        // 每10秒触发一次输出检查
        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
        Map<String, Long> pathCounts = pathCountsState.value();
        if (pathCounts != null && !pathCounts.isEmpty()) {
            List<Map.Entry<String, Long>> sortedPaths = new ArrayList<>(pathCounts.entrySet());
            sortedPaths.sort((e1, e2) -> Long.compare(e2.getValue(), e1.getValue()));

            String currentOutput = buildOutputString(sortedPaths);
            String lastOutput = lastOutputState.value();

            if (!currentOutput.equals(lastOutput)) {
                String currentDate = java.time.LocalDate.now().toString();
                out.collect(Tuple2.of("======= " + currentDate + " 更新TOP" + topN + "热门路径 =======", 0L));

                int count = 0;
                for (Map.Entry<String, Long> entry : sortedPaths) {
                    if (count >= topN) break;

                    // 直接输出带日期的路径
                    String datedPath = entry.getKey();
                    out.collect(Tuple2.of(datedPath, entry.getValue()));
                    count++;
                }

                lastOutputState.update(currentOutput);
            }
        }
    }

    /**
     * 构建输出内容的字符串表示，用于比较是否发生变化
     */
    private String buildOutputString(List<Map.Entry<String, Long>> sortedPaths) {
        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (Map.Entry<String, Long> entry : sortedPaths) {
            if (count >= topN) break;
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
            count++;
        }
        return sb.toString();
    }
}