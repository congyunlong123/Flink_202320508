package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//侧输出流
public class Exercise3 {
    //定义侧输出流标签
    private static OutputTag<String> outputTag = new OutputTag<String>("侧输出流"){};
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<String> result = source.map(new RichMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] words = s.split(" ");
                return Tuple2.of(words[0], Long.parseLong(words[1]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> tuple2, long l) {
                        return tuple2.f1;
                    }
                })).process(new ProcessFunction<Tuple2<String, Long>, String>() {
            @Override
            public void processElement(Tuple2<String, Long> tuple2, Context context, Collector<String> collector) throws Exception {
                if (tuple2.f1 < context.timerService().currentWatermark()) {
                    //如果事件时间小于水位线，则是迟到数据
                    context.output(outputTag, "迟到数据是：" + tuple2);
                } else {
                    collector.collect("正常数据是：" + tuple2);
                }
            }
        });

        result.print();//打印正常数据
        result.getSideOutput(outputTag).print();//打印侧输出流数据


        env.execute();


    }

}
