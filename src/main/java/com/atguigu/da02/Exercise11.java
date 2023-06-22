package com.atguigu.da02;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.awt.*;

public class Exercise11 {

    public static void main(String[] args) throws Exception{
        //使用富函数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Exercise1.Event> source = env.addSource(new Exercise1.ClikcSource());

        source.map(new RichMapFunction<Exercise1.Event, String>() {

            @Override
            public String map(Exercise1.Event event) throws Exception {
                return event.toString();
            }
        }).print();


        env.execute();


    }


}
