package com.yifeng.functions;

import com.yifeng.data.DataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * Created by guoyifeng on 7/23/18
 */
public class SquareWaveFunction extends RichMapFunction<DataPoint<Double>, DataPoint<Double>> {

    @Override
    public DataPoint<Double> map(DataPoint<Double> dataPoint) throws Exception {
        double phase = 0.0;
        if (dataPoint.getValue() > 0.4) {
            phase = 1;
        }
        return dataPoint.withNewValue(phase);
    }
}
