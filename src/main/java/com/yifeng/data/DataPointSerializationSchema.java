package com.yifeng.data;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Created by guoyifeng on 7/22/18
 */
public class DataPointSerializationSchema implements SerializationSchema<KeyedDataPoint<Double>> , DeserializationSchema<KeyedDataPoint<Double>> {

    public byte[] serialize(KeyedDataPoint<Double> dataPoint) {
        String s = dataPoint.getTimeStampMs() + "," + dataPoint.getKey() + "," + dataPoint.getValue();
        return s.getBytes();
    }

    public KeyedDataPoint<Double> deserialize(byte[] bytes) throws IOException {
        String s = new String(bytes);
        String[] tokens = s.split(",");
        long timeStampMs = Long.parseLong(tokens[0]);
        String key = tokens[1];
        double value = Double.parseDouble(tokens[2]);
        return new KeyedDataPoint<Double>(key, timeStampMs, value);
    }

    public boolean isEndOfStream(KeyedDataPoint<Double> doubleKeyedDataPoint) {
        return false;
    }

    public TypeInformation<KeyedDataPoint<Double>> getProducedType() {
        return TypeInformation.of(new TypeHint<KeyedDataPoint<Double>>() {});
    }
}
