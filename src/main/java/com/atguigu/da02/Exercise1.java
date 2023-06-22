package com.atguigu.da02;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import scala.math.Ordering;


import java.awt.*;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;

//自定义数据源
public class Exercise1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClikcSource()).print();

        env.execute();

    }


    public static class  ClikcSource implements SourceFunction<Event>{

        private boolean isFlag = true;
        private String[] userArr = {"Mary", "Alice", "Bob", "Liz"};
        private String[] urlArr = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
        private Random random = new Random() ;
        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            while(isFlag){
                String user = userArr[random.nextInt(3)];
                String url = urlArr[random.nextInt(4)];
                sourceContext.collect(new Event(user,url,Calendar.getInstance().getTimeInMillis()));
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            isFlag =false;
        }
    }

    //POJO类
    //1.公共的属性方法，公共类，无参构造器
    public static class Event{

        public String user;
        public String url;
        public Long timestamp;

        public Event() {
        }

        public Event(String user, String url, Long timestamp) {
            this.user = user;
            this.url = url;
            this.timestamp = timestamp;
        }

        public Event of(String user,String url,Long timestamp){
            return new Event(user,url,timestamp);
        }
        @Override
        public String toString() {
            return "Event{" +
                    "user='" + user + '\'' +
                    ", url='" + url + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }



}














