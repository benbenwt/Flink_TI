package com.ti.utils.serializer;

import com.ti.domain.SecurityInfo;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;


public class KafkaSecuritySerializer implements Serializer<SecurityInfo> {

    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public byte[] serialize(String s, SecurityInfo securityInfo) {
        return   SerializeUtil.serialize(securityInfo);
    }

    public byte[] serialize(String topic, Headers headers, SecurityInfo data) {
        return   SerializeUtil.serialize(data);
    }

    public void close() {

    }
}
