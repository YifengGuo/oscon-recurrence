package com.yifeng.functions;

import com.yifeng.data.DataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * Created by guoyifeng on 7/23/18
 */

/**
 * input should be sawtooth wave which gives a sequence of float number between [0 ,1)
 */
public class SineWaveFunction extends RichMapFunction<DataPoint<Double>, DataPoint<Double>> {
    @Override
    public DataPoint<Double> map(DataPoint<Double> dataPoint) throws Exception {
        double phase = dataPoint.getValue() * 2 * Math.PI; // convert value of sawtooth phase into angle
        return dataPoint.withNewValue(Math.sin(phase)); // generate sin wave given angle
    }
}
