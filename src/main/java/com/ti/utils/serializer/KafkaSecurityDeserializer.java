package com.ti.utils.serializer;

import com.ti.domain.SecurityInfo;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;
import java.util.Map;

public class KafkaSecurityDeserializer implements Deserializer<SecurityInfo> {
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public SecurityInfo deserialize(String topic, byte[] data) {
        return (SecurityInfo)SerializeUtil.deserialize(data,SecurityInfo.class);
    }

    public SecurityInfo deserialize(String topic, Headers headers, byte[] data) {
        return (SecurityInfo)SerializeUtil.deserialize(data,SecurityInfo.class);
    }

    public void close() {

    }
}
