package org.apache.flink.streaming.examples.my;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * @author xinghailong at 2020-09-28 1:52 下午
 */
public class Test {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        env.socketTextStream("localhost", 12345, "\n")
                // 格式化事件
                .map((MapFunction<String, Bean>) s -> {
                    try {
                        LocalDateTime time = LocalDateTime.parse(s, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                        return new Bean(s+"的事件", time.toInstant(ZoneOffset.of("+8")).toEpochMilli());
                    } catch (Exception e) {}
                    return null;
                })
                .filter(Objects::nonNull)
                // 定义事件水印
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Bean>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(Bean bean) {
                        System.out.println("bean name="+bean.name+" time="+bean.time);
                        return bean.getTime();
                    }
                })
                // 配置窗口
                .timeWindowAll(Time.minutes(1))
                // debug默认触发器
                .trigger(new EventTimeTrigger())
                // 应用
                .apply(
                        new AllWindowFunction<Bean, Bean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow timeWindow, Iterable<Bean> iterable,Collector<Bean> collector) throws Exception {
                                for (Bean signalDTO : iterable) {
                                    collector.collect(signalDTO);
                                }

                            }
                        })
                .print();

        env.execute("Socket Window WordCount");
    }

    static class EventTimeTrigger extends Trigger<Object, TimeWindow> {
        private static final long serialVersionUID = 1L;

        private EventTimeTrigger() {}

        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            System.out.println("事件事件=" + new Timestamp(timestamp).toLocalDateTime()
                    +" 当前水印=" + new Timestamp(ctx.getCurrentWatermark()).toLocalDateTime()
                    +" 窗口时间=" + new Timestamp(window.maxTimestamp()).toLocalDateTime()
            );
            if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                // if the watermark is already past the window fire immediately
                return TriggerResult.FIRE;
            } else {
                ctx.registerEventTimeTimer(window.maxTimestamp());
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            System.out.println("time=" + new Timestamp(time).toLocalDateTime()
                    +" 当前水印=" + new Timestamp(ctx.getCurrentWatermark()).toLocalDateTime()
                    +" 窗口时间=" + new Timestamp(window.maxTimestamp()).toLocalDateTime());
            return time == window.maxTimestamp() ?
                    TriggerResult.FIRE :
                    TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.deleteEventTimeTimer(window.maxTimestamp());
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(TimeWindow window,
                            OnMergeContext ctx) {
            // only register a timer if the watermark is not yet past the end of the merged window
            // this is in line with the logic in onElement(). If the watermark is past the end of
            // the window onElement() will fire and setting a timer here would fire the window twice.
            long windowMaxTimestamp = window.maxTimestamp();
            if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
                ctx.registerEventTimeTimer(windowMaxTimestamp);
            }
        }

        @Override
        public String toString() {
            return "EventTimeTrigger()";
        }

        /**
         * Creates an event-time trigger that fires once the watermark passes the end of the window.
         *
         * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
         * trigger window evaluation with just this one element.
         */
        public static EventTimeTrigger create() {
            return new EventTimeTrigger();
        }
    }

    static class Bean {
        private String name;
        private Long time;

        public Bean(String name, Long time) {
            this.name = name;
            this.time = time;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Long getTime() {
            return time;
        }

        public void setTime(Long time) {
            this.time = time;
        }
    }
}
