package com.ti.dws;

import com.ti.domain.SecurityInfo;
import com.ti.functions.map.ParseJsonMapFunction;
import com.ti.utils.serializer.FlinkSecurityInfoDeserializationSchema;
import com.ti.utils.serializer.FlinkSecurityInfoSerializationSchema;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class LocationDayPeriod {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("groupid","dws_LocationDayPeriod");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");

        DataStream<SecurityInfo> infoStream=env.addSource(new FlinkKafkaConsumer<SecurityInfo>("dwd_ParseJson",new FlinkSecurityInfoDeserializationSchema<SecurityInfo>(SecurityInfo.class), properties));
//        DataStream<SecurityInfo> infoStream=kafkaStream.map(str -> {
//            SecurityInfo info=new SecurityInfo();
//            info.setLocation("ch6");
//            info.setSampletime("2022-02-02");
//            info.setType("worm");
//            info.setArchitecture("x86");
//            return info;
//        });

//如果用DataStream实现table的group by，需要使用keyedProcessFunction，因为聚合函数必须要开窗。
        StreamTableEnvironment tableEnv=StreamTableEnvironment.create(env);
        Table infoTable=tableEnv.fromDataStream(infoStream);
        infoTable.printSchema();
        tableEnv.toChangelogStream(infoTable).print("infoTable:");
        Table countLocationTable=tableEnv.sqlQuery("select location,sampletime,count(location) from "+infoTable+" group by location,sampletime");
        Table countTypeTable=tableEnv.sqlQuery("select type,sampletime,count(location) from "+infoTable+" group by type,sampletime");
        Table countArchTable=tableEnv.sqlQuery("select architecture,sampletime,count(location) from "+infoTable+" group by architecture,sampletime");
        tableEnv.toChangelogStream(countLocationTable).print("countLocationTable: ");
        tableEnv.toChangelogStream(countTypeTable).print("countTypeTable:");
        tableEnv.toChangelogStream(countArchTable).print("countArchTable:");
//  所有数据中，每天分布了多少个。
        env.execute("locationDayPeriod");
    }
}
