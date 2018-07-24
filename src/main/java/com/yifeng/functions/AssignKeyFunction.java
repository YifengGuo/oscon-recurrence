package com.yifeng.functions;

import com.yifeng.data.DataPoint;
import com.yifeng.data.KeyedDataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * Created by guoyifeng on 7/23/18
 */
public class AssignKeyFunction extends RichMapFunction<DataPoint<Double>, KeyedDataPoint<Double>> {

    private String key;

    public AssignKeyFunction(String key) {
        this.key = key;
    }

    @Override
    public KeyedDataPoint<Double> map(DataPoint<Double> dataPoint) throws Exception {
        return dataPoint.withNewKey(key);
    }
}
