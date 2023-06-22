package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.concurrent.java8.FuturesConvertersImpl;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

//实时热门商品
public class Exercise6 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("D:\\2021-4-28\\Flink_202320508\\src\\main\\resources\\UserBehavior.csv");
        source.map(new RichMapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                String[] words = s.split(",");
                return new UserBehavior(
                        words[0],
                        words[1],
                        words[2],
                        words[3],
                        Long.parseLong(words[4])*1000L
                );
            }
        }).filter(r->r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior userBehavior, long l) {
                        return userBehavior.timeStamp;
                    }
                })).windowAll(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))
                .process(new WindowResult())
                .print();


        env.execute();


    }

    public static class WindowResult extends ProcessAllWindowFunction<UserBehavior,String,TimeWindow>{

        @Override
        public void process(Context context, Iterable<UserBehavior> iterable, Collector<String> collector) throws Exception {
            //创建一个hashmap存放商品ID和浏览次数
            HashMap<String, Long> map = new HashMap<>();
            //将迭代器内的元素取出，判断map里是否包含该商品，如果不包含则添加，如果包含则浏览次数+1
            for(UserBehavior user : iterable){
                if(!map.containsKey(user.itemId)){
                    map.put(user.itemId,1L);
                }else {
                    map.put(user.itemId,map.get(user.itemId)+1L);
                }
            }

            //创建一个arraylist列表用于存放map中的元素进行排序
            ArrayList<Tuple2<String, Long>> arrayList = new ArrayList<>();

            for(String key:map.keySet()){
                arrayList.add(Tuple2.of(key,map.get(key)));
            }

            //进行倒序排序
            arrayList.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    return o2.f1.intValue()-o1.f1.intValue();
                }
            });

            //输出结果
            StringBuilder result = new StringBuilder();
            result.append("---------------------------------------\n");
            String windowStart = new Timestamp(context.window().getStart()).toString();
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            result.append("窗口时间：" + windowStart+"~"+ windowEnd+"内\n");
            for(int i =0;i<3;i++){
                result.append("窗口内浏览量第"+i+"名商品：" + arrayList.get(i).f0+"浏览次数是：" + arrayList.get(i).f1+"\n");
            }

            collector.collect(result.toString());

        }
    }


    public static class UserBehavior{
        public String userID;
        public String itemId;
        public String categoryId;
        public String behavior;
        public Long timeStamp;


        public UserBehavior() {
        }

        public UserBehavior(String userID, String itemId, String categoryId, String behavior, Long timeStamp) {
            this.userID = userID;
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.behavior = behavior;
            this.timeStamp = timeStamp;
        }

        @Override
        public String toString() {
            return "UserBehavior{" +
                    "userID='" + userID + '\'' +
                    ", itemId='" + itemId + '\'' +
                    ", categoryId='" + categoryId + '\'' +
                    ", behavior='" + behavior + '\'' +
                    ", timeStamp=" + new Timestamp(timeStamp) +
                    '}';
        }
    }


}

