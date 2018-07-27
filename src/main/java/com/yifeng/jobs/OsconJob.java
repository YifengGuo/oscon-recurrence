package com.yifeng.jobs;

import com.yifeng.data.DataPoint;
import com.yifeng.data.KeyedDataPoint;
import com.yifeng.functions.AssignKeyFunction;
import com.yifeng.functions.SawtoothFunction;
import com.yifeng.functions.SineWaveFunction;
import com.yifeng.functions.SquareWaveFunction;
import com.yifeng.sink.InfluxDBSink;
import com.yifeng.sources.TimestampSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.influxdb.InfluxDB;

/**
 * Created by guoyifeng on 7/23/18
 */
public class OsconJob {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Set fault-tolerance for state as 1 second
        env.enableCheckpointing(1000);

        // Set default time characteristic as event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Simulate some sensor data
        DataStream<KeyedDataPoint<Double>> sensorStream = generateSensorData(env);

        // Write this sensor stream out to InfluxDB
        sensorStream
                .addSink(new InfluxDBSink<>("sensors"));

        // Compute a windowed sum over this data and write that to InfluxDB as well.
        sensorStream
                .keyBy("key")
                .timeWindow(Time.seconds(1))
                .sum("value")
                .addSink(new InfluxDBSink<>("summedSensors"));

        env.execute("flink demo");


    }

    private static DataStream<KeyedDataPoint<Double>> generateSensorData(StreamExecutionEnvironment env) {
        // boilerplate for this demo
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1000, 1000));
        env.setMaxParallelism(8);
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.getConfig().setLatencyTrackingInterval(1000);

        final int SLOWDOWN_FACTOR = 1;
        final int PERIOD_MS = 100;

        // Initial data - just timestamped messages
        DataStreamSource<DataPoint<Long>> timestampSource =
                env.addSource(new TimestampSource(PERIOD_MS, SLOWDOWN_FACTOR), "test data");

        // Transform initial data into sawtooth pattern
        SingleOutputStreamOperator<DataPoint<Double>> sawtoothStream = timestampSource
                .map(new SawtoothFunction(10))
                .name("sawTooth");

        // Simulate temperature sensor
        // adding key to data points in sawtoothStream
        SingleOutputStreamOperator<KeyedDataPoint<Double>> tempStream = sawtoothStream
                .map(new AssignKeyFunction("temp"))
                .name("assignKey(temp)");

        // make sine wave and use for pressure sensor
        SingleOutputStreamOperator<KeyedDataPoint<Double>> pressureStream = sawtoothStream
                .map(new SineWaveFunction())
                .name("sineWave")
                .map(new AssignKeyFunction("pressure"))
                .name("assignKey(pressure)");

        // make square wave and use for door sensor
        SingleOutputStreamOperator<KeyedDataPoint<Double>> doorStream = sawtoothStream
                .map(new SquareWaveFunction())
                .name("squareWave")
                .map(new AssignKeyFunction("door"))
                .name("assignKey(door)");

        // Combine all the streams into one and write it to Kafka
        DataStream<KeyedDataPoint<Double>> sensorStream = tempStream
                .union(pressureStream)
                .union(doorStream);

        return sensorStream;
    }
}
