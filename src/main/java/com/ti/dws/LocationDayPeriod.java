package com.ti.dws;

import com.ti.domain.SecurityInfo;
import com.ti.functions.map.ParseJsonMapFunction;
import com.ti.utils.serializer.FlinkSecurityInfoDeserializationSchema;
import com.ti.utils.serializer.FlinkSecurityInfoSerializationSchema;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
        Table countLocationTable=tableEnv.sqlQuery("select location,substring(sampletime,0,7) as year_month,count(sampletime) as nums from "+infoTable+" where sampletime <> '' and location <> '' group by location , substring(sampletime,0,7)");
        Table countTypeTable=tableEnv.sqlQuery("select `type`,substring(sampletime,0,7) as year_month,count(sampletime) as nums from "+infoTable+" where sampletime <> '' and `type` <> '' group by `type` , substring(sampletime,0,7)");
        Table countArchTable=tableEnv.sqlQuery("select `architecture`,substring(sampletime,0,7) as year_month,count(sampletime) as nums from "+infoTable+" where sampletime <> '' and `architecture` <> ''  group by `architecture` , substring(sampletime,0,7)");

        DataStream<Row> countLocationStream=tableEnv.toChangelogStream(countLocationTable);
        DataStream<Row> countTypeStream=tableEnv.toChangelogStream(countTypeTable);
        DataStream<Row> countArchStream=tableEnv.toChangelogStream(countArchTable);

        countLocationStream.print();
        countTypeStream.print();
        countArchStream.print();

//        sink location
        ElasticsearchSinkFunction<Row> elasticsearchSinkFunction=new ElasticsearchSinkFunction<Row>() {
            @Override
            public void process(Row element, RuntimeContext ctx, RequestIndexer indexer) {
                HashMap<String,String> data=new HashMap<>();
                data.put("location", (String) element.getField(0));
                data.put("year_month", (String) element.getField(1));
                data.put("nums", String.valueOf(element.getField(2)));

                IndexRequest request= Requests.indexRequest().index("locationmonth").source(data).id((String) element.getField(0)+"_"+(String) element.getField(1));
                indexer.add(request);
            }
        };
        List<HttpHost> httpHosts=new ArrayList<>();
        httpHosts.add(new HttpHost("192.168.31.177",9201,"http"));
        ElasticsearchSink.Builder<Row> builder=new ElasticsearchSink.Builder<Row>(httpHosts, elasticsearchSinkFunction);
        builder.setBulkFlushMaxActions(1);
        countLocationStream.addSink(builder.build());

//       sink type
        ElasticsearchSinkFunction<Row> elasticsearchSinkFunction1=new ElasticsearchSinkFunction<Row>() {
            @Override
            public void process(Row element, RuntimeContext ctx, RequestIndexer indexer) {
                HashMap<String,String> data=new HashMap<>();
                data.put("type", (String) element.getField(0));
                data.put("year_month", (String) element.getField(1));
                data.put("nums", String.valueOf(element.getField(2)));

                IndexRequest request= Requests.indexRequest().index("typemonth").source(data).id((String) element.getField(0)+"_"+(String) element.getField(1));
                indexer.add(request);
            }
        };
        List<HttpHost> httpHosts1=new ArrayList<>();
        httpHosts1.add(new HttpHost("192.168.31.177",9201,"http"));
        ElasticsearchSink.Builder<Row> builder1=new ElasticsearchSink.Builder<Row>(httpHosts1, elasticsearchSinkFunction1);
        builder1.setBulkFlushMaxActions(1);
        countTypeStream.addSink(builder1.build());
//        sink architecture
        ElasticsearchSinkFunction<Row> elasticsearchSinkFunction2=new ElasticsearchSinkFunction<Row>() {
            @Override
            public void process(Row element, RuntimeContext ctx, RequestIndexer indexer) {
                HashMap<String,String> data=new HashMap<>();
                data.put("architecture", (String) element.getField(0));
                data.put("year_month", (String) element.getField(1));
                data.put("nums", String.valueOf(element.getField(2)));

                IndexRequest request= Requests.indexRequest().index("archmonth").source(data).id((String) element.getField(0)+"_"+(String) element.getField(1));
                indexer.add(request);
            }
        };
        List<HttpHost> httpHosts2=new ArrayList<>();
        httpHosts2.add(new HttpHost("192.168.31.177",9201,"http"));
        ElasticsearchSink.Builder<Row> builder2=new ElasticsearchSink.Builder<Row>(httpHosts2, elasticsearchSinkFunction2);
        builder2.setBulkFlushMaxActions(1);
        countArchStream.addSink(builder2.build());
//  所有数据中，每天分布了多少个。
        env.execute("locationDayPeriod");
    }
}
