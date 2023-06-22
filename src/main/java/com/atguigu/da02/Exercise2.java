package com.atguigu.da02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Exercise2 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        DataStreamSource<Exercise1.Event> sourceStream = env.addSource(new Exercise1.ClikcSource());

        DataStreamSink<String> reduce = sourceStream.map(new MapFunction<Exercise1.Event, String>() {
            @Override
            public String map(Exercise1.Event event) throws Exception {
                return event.user;
            }
        }).print();


        DataStreamSink<String> reduce1 = env.addSource(new Exercise1.ClikcSource()).map(r -> r.user).print();


        env.execute();

    }

}
