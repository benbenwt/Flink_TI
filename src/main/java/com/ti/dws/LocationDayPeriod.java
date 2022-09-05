package com.ti.dws;

import com.ti.domain.SecurityInfo;
import com.ti.functions.map.ParseJsonMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class LocationDayPeriod {
    public static void main(String[] args) {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties=new Properties();
        properties.setProperty("bootstrap.server","localhost:9092");
        properties.setProperty("groupid","dws_LocationDayPeriod");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");

        DataStream<String> kafkaStream=env.addSource(new FlinkKafkaConsumer<String>("dwd_ParseJson",new SimpleStringSchema(), properties));
        DataStream<SecurityInfo> infoStream=kafkaStream.map(new ParseJsonMapFunction());
    }
}
