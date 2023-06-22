package com.atguigu.da02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Exercise3 {

    public static void main(String[] args) throws Exception{

        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Exercise1.Event> source = env.addSource(new Exercise1.ClikcSource());

        source.filter(new RichFilterFunction<Exercise1.Event>() {
            @Override
            public boolean filter(Exercise1.Event event) throws Exception {
                return event.user.equals("Mary");

            }
        }).print();

        source.flatMap(new FlatMapFunction<Exercise1.Event, String>() {
            @Override
            public void flatMap(Exercise1.Event event, Collector<String> collector) throws Exception {
                if(event.user.equals("Mary")){
                    collector.collect(event.toString());
                }
     
            }
        }).print();

        env.execute();


    }




}
