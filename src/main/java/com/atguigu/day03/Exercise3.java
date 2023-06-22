package com.atguigu.day03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Random;

public class Exercise3 {
    //使用keyprocressFunction计算平均数
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new SourceFunction<Integer>() {
            private Random random = new Random();
            private boolean isflag = true;
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                while (isflag) {
                    sourceContext.collect(random.nextInt(1000));
                    Thread.sleep(1000L);
                }

            }

            @Override
            public void cancel() {
                isflag = false;
            }
        }).map(new RichMapFunction<Integer, Tuple2<Integer,Long>>() {
            @Override
            public Tuple2<Integer, Long> map(Integer integer) throws Exception {
                return Tuple2.of(integer,1L);
            }
        })
                .keyBy(r->r.f0)
                .process(new KeyedProcessFunction<Integer, Tuple2<Integer, Long>, Double>() {

                    //创建一个累加器值状态变量
                    private ValueState<Tuple2<Integer,Long>> sumcount;
                    //创建一个定时器状态变量
                    private ValueState<Long> count ;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //对状态变量进行初始化
                        sumcount = getRuntimeContext().getState(
                                new ValueStateDescriptor<Tuple2<Integer, Long>>("sum-count",Types.TUPLE(Types.INT,Types.LONG)));
                        count = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count",Types.LONG));
                    }

                    @Override
                    public void processElement(Tuple2<Integer, Long> values, Context context, Collector<Double> collector) throws Exception {
                        //首先判断累加器的是是否是空，如果为空则是第一条数据，否则不是
                        if(sumcount.value() == null){
                            sumcount.update(Tuple2.of(values.f0,values.f1));
                        }else {
                            Tuple2<Integer, Long> tmp = new Tuple2<>();
                            tmp = sumcount.value();
                            sumcount.update(Tuple2.of(tmp.f0+sumcount.value().f0,tmp.f1+sumcount.value().f1));
                        }
                        //创建一个10Sde
                        //创建一个10S的定时器
                        if(count.value() == null){
                            count.update (context.timerService().currentProcessingTime());;
                            context.timerService().registerProcessingTimeTimer(count.value());
                        }


                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Double> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect((double)sumcount.value().f0/sumcount.value().f1);
                        count.clear();
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        //清空累加器
                        sumcount.clear();

                    }
                }).print();




        env.execute();

    }
// 状态变量用来保存定时器的时间戳，定时器的作用是向下游发送平均值

}
