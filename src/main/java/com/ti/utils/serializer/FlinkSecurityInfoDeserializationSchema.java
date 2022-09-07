package com.ti.utils.serializer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class FlinkSecurityInfoDeserializationSchema<T> implements DeserializationSchema<T> {
    private Class<T> clazz;
    public FlinkSecurityInfoDeserializationSchema(Class<T> clazz){
        this.clazz=clazz;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        return (T)SerializeUtil.deserialize(message,clazz);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeExtractor.getForClass(clazz);
    }
}
