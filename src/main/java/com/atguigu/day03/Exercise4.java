package com.atguigu.day03;

import javafx.beans.binding.DoubleExpression;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.security.Principal;
import java.util.Random;

public class Exercise4 {
    //连续1S温度上升
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        env.addSource(new SourceFunction<SensorReading>() {
            private Random random = new Random();
            private boolean isflag = true;
            @Override
            public void run(SourceContext<SensorReading> sourceContext) throws Exception {
                while(isflag){
                    for(int i =0;i<3;i++){
                        sourceContext.collect(new SensorReading(
                                "sensor"+(i+1),
                                random.nextGaussian()
                        ));
                    }
                    Thread.sleep(1000L);

                }
                
            }

            @Override
            public void cancel() {
                isflag = false;
            }
        }).keyBy(r->r.sensorId).process(new KeyedProcessFunction<String, SensorReading, String>() {
            private ValueState<Double> lasttMP;
            private  ValueState<Long> onTimers;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                lasttMP = getRuntimeContext().getState(new ValueStateDescriptor<Double>("最新的温度",Types.DOUBLE));
                onTimers = getRuntimeContext().getState(new ValueStateDescriptor<Long>("定时器",Types.LONG));
            }

            @Override
            public void processElement(SensorReading sensorReading, Context context, Collector<String> collector) throws Exception {
                //首先判断温度状态器内有没有值，如果没有则说明是第一次来数据
                Double pervTmp = null;
                if(lasttMP.value()==null){
                    lasttMP.update(sensorReading.temperature);
                }
                pervTmp = lasttMP.value();

                Long ts = null;
                if(onTimers.value()!=null){
                    ts = onTimers.value();
                }

                if(pervTmp == null || pervTmp > sensorReading.temperature){
                    if(ts!=null){
                        context.timerService().deleteProcessingTimeTimer(ts);
                        onTimers.clear();
                    }
                }else if(sensorReading.temperature>pervTmp &&ts ==null){
                    context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime()+1000L);
                }

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                out.collect("传感器：" + ctx.getCurrentKey()+"连续1秒钟温度上升了");
                onTimers.clear();
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        }).print("测试test");

        System.out.println("hot-fix");
        System.out.println("测试分支冲突");
        System.out.println("测试分支冲突");
        System.out.println("测试分支冲突");
        env.execute();
        
        

    }

    public static class SensorReading{
        public String sensorId;
        public Double temperature;

        public SensorReading() {
        }

        public SensorReading(String sensorId, Double temperature) {
            this.sensorId = sensorId;
            this.temperature = temperature;
        }

        @Override
        public String toString() {
            return "SensorReading{" +
                    "sensorId='" + sensorId + '\'' +
                    ", temperature=" + temperature +
                    '}';
        }
    }


}
