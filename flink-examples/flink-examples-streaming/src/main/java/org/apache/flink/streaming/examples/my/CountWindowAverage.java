package org.apache.flink.streaming.examples.my;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author xingoo
 */
public class CountWindowAverage {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        env.fromElements(Tuple2.of(1, 2), Tuple2.of(1, 5), Tuple2.of(1, 7), Tuple2.of(1, 4), Tuple2.of(1, 2), Tuple2.of(1, 2))
                .keyBy(0)
                .flatMap(new CountWindowAvg())
                .print();

        env.execute("avg");
    }

    static class CountWindowAvg extends RichFlatMapFunction <Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

        private transient ValueState<Tuple2<Integer, Integer>> sum;

        @Override
        public void flatMap(Tuple2<Integer, Integer> in, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            Tuple2<Integer, Integer> currentSum = sum.value();

            currentSum.f0 += 1;
            currentSum.f1 += in.f1;

            sum.update(currentSum);

            if(currentSum.f0 >= 2) {
                out.collect(new Tuple2<>(in.f0, currentSum.f1/currentSum.f0));
                sum.clear();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            ValueStateDescriptor<Tuple2<Integer, Integer>> desc = new ValueStateDescriptor<>(
                    "avg",
                    TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
            }));

            // 只要有desc就可以获取对应的状态
            sum = getRuntimeContext().getState(desc);

        }
    }
}
