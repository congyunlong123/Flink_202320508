package com.atguigu.day03;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class Exercise2 {
        //keyprocseeFunction
        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);


            DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

            source.flatMap(new RichFlatMapFunction<String, Tuple2<String ,Long>>() {
                @Override
                public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                    String[] words = s.split(" ");

                    for(String word:words){
                        collector.collect(Tuple2.of(word,1L));
                    }

                }
            }).keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                @Override
                public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                    return tuple2.f0;
                }
            }).process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                @Override
                public void processElement(Tuple2<String, Long> tuple2, Context context, Collector<String> collector) throws Exception {
                    collector.collect("key到达的时间是：" + new Timestamp(context.timerService().currentProcessingTime()));
                    collector.collect("到达的key是："+context.getCurrentKey());
                    //注册定时器
                    context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime());
                    collector.collect("注册一个时间为："+ new Timestamp(context.timerService().currentProcessingTime())+"的定时器");

                }

                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                    super.onTimer(timestamp, ctx, out);
                    out.collect("定时器被触发了"+new Timestamp(timestamp));
                }
            }).print();

            env.execute();
        }
}
