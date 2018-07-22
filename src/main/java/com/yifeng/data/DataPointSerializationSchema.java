package com.yifeng.data;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by guoyifeng on 7/22/18
 */
public class DataPointSerializationSchema implements SerializationSchema<KeyedDataPoint<Double>> , DeserializationSchema<KeyedDataPoint<Double>> {

    public byte[] serialize(KeyedDataPoint<Double> doubleKeyedDataPoint) {
        return new byte[0];
    }

    public KeyedDataPoint<Double> deserialize(byte[] bytes) throws IOException {
        return null;
    }

    public boolean isEndOfStream(KeyedDataPoint<Double> doubleKeyedDataPoint) {
        return false;
    }

    public TypeInformation<KeyedDataPoint<Double>> getProducedType() {
        return null;
    }
}
