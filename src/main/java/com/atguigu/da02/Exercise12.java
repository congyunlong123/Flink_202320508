package com.atguigu.da02;

import com.mysql.jdbc.Driver;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;

import java.util.Properties;


//将数据写入到mysql中
public class Exercise12 {

    public static void main(String[] args) throws  Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Exercise1.Event> source = env.addSource(new Exercise1.ClikcSource());

        source.addSink(new RichSinkFunction<Exercise1.Event>() {
            //创建简介器
            private Connection conn ;
            private Properties properties  = new Properties();
            private PreparedStatement inputStatement;
            private PreparedStatement outputStatement;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println(properties);

                properties.setProperty("url","jdbc:mysql://hadoop102:3306/ad");
                properties.setProperty("user","root");
                properties.setProperty("password","123456");

                conn = DriverManager.getConnection(properties.getProperty("url"),properties.getProperty("user"),properties.getProperty("password"));
                inputStatement = conn.prepareStatement("INSERT INTO clicks (user, url) VALUES (?, ?)");
                outputStatement = conn.prepareStatement("UPDATE clicks SET url = ? WHERE user = ?");
            }

            @Override
            public void invoke(Exercise1.Event value, Context context) throws Exception {

                if(outputStatement.getUpdateCount() ==0){
                    inputStatement.setObject(1,value.user);
                    inputStatement.setObject(2,value.url);
                    inputStatement.execute();
                }else{
                    outputStatement.setObject(1,value.url);
                    outputStatement.setObject(2,value.user);
                    outputStatement.execute();
                }


            }

            @Override
            public void close() throws Exception {
                super.close();
                conn.close();
                inputStatement.close();
                outputStatement.close();
            }
        });

        env.execute();

    }

}




