package com.yifeng.functions;

import com.yifeng.data.DataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import java.util.Collections;
import java.util.List;

/**
 * Created by guoyifeng on 7/23/18
 */
public class SawtoothFunction extends RichMapFunction<DataPoint<Long>, DataPoint<Double>> implements ListCheckpointed<Integer> {
    final private int numSteps;
    private Counter datapoints; // counter to count DataPoint
    // this is the State for this operator
    private int currentStep;


    public SawtoothFunction(int numSteps) {
        this.numSteps = numSteps;
        this.currentStep = 0;
    }

    @Override
    public void open(Configuration config) {
        // initialize Counter
        this.datapoints = getRuntimeContext()
                .getMetricGroup()
                .counter("datapoints");
    }

    /**
     * map the input DataPoint with new value following sawtooth function
     * example: if numSteps == 3:
     *     the result would be like: 0.0 0.3333333333333333 0.6666666666666666
     *                               0.0 0.3333333333333333 0.6666666666666666
     *                               0.0 0.3333333333333333 0.6666666666666666 0.0 ...
     * @param dataPoint
     * @return
     * @throws Exception
     */
    @Override
    public DataPoint<Double> map(DataPoint<Long> dataPoint) throws Exception {
        double phase = (double)currentStep / numSteps;
        currentStep = ++currentStep % numSteps; // reset currentStep to 0 if it equals numSteps
        this.datapoints.inc();
        return dataPoint.withNewValue(phase);
    }

    public List<Integer> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
        return Collections.singletonList(currentStep);
    }

    public void restoreState(List<Integer> state) throws Exception {
        for (Integer s : state) {
            this.currentStep = s;
        }
    }
}
