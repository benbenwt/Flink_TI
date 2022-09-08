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
        infoStream.map(info ->{
            System.out.println(info);
            return info;
        });

        StreamTableEnvironment tableEnv=StreamTableEnvironment.create(env);
        Table infoTable=tableEnv.fromDataStream(infoStream);
        infoTable.printSchema();
//        tableEnv.toChangelogStream(infoTable).print("infoTable:");
//        这样key的数量等于sampletime数量*location的数量，sampletime随时间增加，key会越来越多。可以通过一段时间后重开应用清空状态。
        Table countLocationTable=tableEnv.sqlQuery("select location,substring(sampletime,0,7) as year_month,count(sampletime) from "+infoTable+" where sampletime <> '' and location <> '' group by location , substring(sampletime,0,7)");
        Table countTypeTable=tableEnv.sqlQuery("select `type`,substring(sampletime,0,7) as year_month,count(sampletime) from "+infoTable+" where sampletime <> '' and `type` <> '' group by `type` , substring(sampletime,0,7)");
        Table countArchTable=tableEnv.sqlQuery("select `architecture`,substring(sampletime,0,7) as year_month,count(sampletime) from "+infoTable+" where sampletime <> '' and `architecture` <> ''  group by `architecture` , substring(sampletime,0,7)");
        tableEnv.toChangelogStream(countLocationTable).print("countLocationTable: ");
        tableEnv.toChangelogStream(countTypeTable).print("countTypeTable:");
        tableEnv.toChangelogStream(countArchTable).print("countArchTable:");
//  所有数据中，每天分布了多少个。
        env.execute("locationDayPeriod");
    }
}
