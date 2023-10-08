package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Exercise2 {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("D:\\2021-4-28\\Flink_202320508\\src\\main\\resources\\word");

        streamSource.flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] words = s.split(" ");

                for (String word :words){

                    collector.collect(Tuple2.of(word,1L));

                }

            }
        }).keyBy(r->r.f0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> tuple2, Tuple2<String, Long> t1) throws Exception {
                return Tuple2.of(tuple2.f0,t1.f1+tuple2.f1);
            }
        }).print();



        env.execute();

    }

}
