import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayList;
import java.util.List;

public class TimeSet {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final List<Tuple3<String, Long, Integer>> inputs = new ArrayList<Tuple3<String, Long, Integer>>();
        inputs.add(Tuple3.of("a", 1L, 1));
        inputs.add(Tuple3.of("b", 1L, 1));
        inputs.add(Tuple3.of("c", 3L, 1));


        /**
         * 在 Source Function 中直接定义 Timestamps 和 Watermark
         */
        DataStream<Tuple3<String, Long, Integer>> dataStream = env.addSource(new SourceFunction<Tuple3<String, Long, Integer>>() {
            public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
                for (Tuple3<String, Long, Integer> input : inputs) {
                    // 抽取 EventTime
                    ctx.collectWithTimestamp(input, input.f1);
                    // 设置 Watermark
                    ctx.emitWatermark(new Watermark(input.f1 - 1));
                }
                // 设置默认 Watermark
                ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
            }

            public void cancel() {

            }
        });

        /**
         * 通过 Flink 自带的 Timestamp Assigner 指定 Timestamp 和 生成 Watermark
         *
         * 根据 Watermark 的生成形式，分为两类：
         * （1）Periodic Watermark：是根据设定时间间隔周期性地生成
         *      -- 升序模式：事件顺序生成，没有乱序，提取最新 Timestamp 为 Watermark，不需要设置
         *      -- 乱序模式：通过固定时间间隔来指定 Watermark 落后于 Timestamp 的区间长度
         * （2）Punctuated Watermark：是根据接入数据的数量生成
         */
        // 在系统中指定 EventTime 概念
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream ds = env.fromCollection(inputs);
        // 使用 Ascending Timestamp Assigner 指定 Timestamp 和 Watermark
//        ds.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor() {
//            @Override
//            public long extractTimestamp(Object element) {
//                return 0;
//            }
//        });
    }
}
