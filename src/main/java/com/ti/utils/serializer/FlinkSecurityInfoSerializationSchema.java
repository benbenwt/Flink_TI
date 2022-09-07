package com.ti.utils.serializer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class FlinkSecurityInfoSerializationSchema<T> implements SerializationSchema<T> {

    @Override
    public byte[] serialize(T element) {
        return SerializeUtil.serialize(element);
    }
}
