package com.atguigu.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Exercise1 {
        //processFunction
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<com.atguigu.da02.Exercise1.Event> source = env.addSource(new com.atguigu.da02.Exercise1.ClikcSource());

        source.process(new ProcessFunction<com.atguigu.da02.Exercise1.Event, String>() {
            @Override
            public void processElement(com.atguigu.da02.Exercise1.Event event, Context context, Collector<String> collector) throws Exception {

                collector.collect("数据到达的时间是："+context.timerService().currentProcessingTime());
                collector.collect("点击事件的用户是："+event.user);
                collector.collect("点击的url是："+event.url);
            }
        }).print();



        env.execute();

    }

}
