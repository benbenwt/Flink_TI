package com.ti.dwd;

import com.ti.domain.SecurityInfo;
import com.ti.functions.map.ParseJsonMapFunction;
import com.ti.utils.serializer.FlinkSecurityInfoSerializationSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class ParseJson {
    public static void main(String[] args) throws Exception {
//        从kafka创建流，元素为json str。
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","dwd_ParseJson");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");

        DataStream<String> kafkaStream=env.addSource(new FlinkKafkaConsumer<String>("ods_json",new SimpleStringSchema(),properties));
        kafkaStream.print("2");

        DataStream<SecurityInfo> infoStream=kafkaStream.map(new ParseJsonMapFunction());

        Properties properties1=new Properties();
        properties1.setProperty("bootstrap.servers","localhost:9092");

        infoStream.addSink(new FlinkKafkaProducer<SecurityInfo>("dwd_ParseJson",new FlinkSecurityInfoSerializationSchema<SecurityInfo>(),properties1));
        env.execute("ParseJson");
    }
}
