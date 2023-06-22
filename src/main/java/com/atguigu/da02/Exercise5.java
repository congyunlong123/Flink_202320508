package com.atguigu.da02;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Exercise5 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //求平均值，最大值最小值，累加

        DataStreamSource source = env.addSource(new IntegerSource());

        source
                .map(r -> Tuple2.of(r, 1))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                // 为每条数据设置同样的key：true，将所有数据分流到同一个逻辑分区
                .keyBy(r -> true)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0 + value2.f0, value1.f1 + value2.f1);
                    }
                })
                .print();
               // .map(r -> r.f0 / r.f1);

        env.execute();


    }

    //创建一个累造数
    public static class IntegerSource implements SourceFunction{

        private boolean running = true;
        private Random random = new Random();
        @Override
        public void run(SourceContext sourceContext) throws Exception {

            while(running){

                sourceContext.collect(random.nextInt(1000));

            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }







}
