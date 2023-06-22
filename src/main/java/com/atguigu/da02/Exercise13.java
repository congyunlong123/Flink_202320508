package com.atguigu.da02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

public class Exercise13 {
        //将数据写出到redis
    public static void main(String[] args)  throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Exercise1.Event> source = env.addSource(new Exercise1.ClikcSource());
        source.addSink(new RichSinkFunction<Exercise1.Event>() {
            private Jedis jedis;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                jedis = new Jedis("hadoop102");

            }

            @Override
            public void invoke(Exercise1.Event value, Context context) throws Exception {

                jedis.set("user",value.user);
                jedis.set("url",value.url);


            }

            @Override
            public void close() throws Exception {
                super.close();
                jedis.close();
            }
        });


        env.execute();





    }



}
