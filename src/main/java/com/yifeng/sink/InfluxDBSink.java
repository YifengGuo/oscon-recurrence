package com.yifeng.sink;

import com.yifeng.data.DataPoint;
import com.yifeng.data.KeyedDataPoint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

/**
 * Created by guoyifeng on 7/23/18
 */
public class InfluxDBSink<T extends DataPoint<? extends Number>> extends RichSinkFunction<T> {
    private transient InfluxDB influxDB = null;
    private static String databaseName = "sineWave";
    private static String fieldName = "value";
    private String measurement;

    public InfluxDBSink(String measurement) {
        this.measurement = measurement;
    }


    /**
     * initialize connection to InfluxDB
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        influxDB = InfluxDBFactory.connect("http://localhost:8086", "admin", "admin");
        influxDB.createDatabase(databaseName);
        influxDB.setDatabase(databaseName);
        // Flush every 2000 Points, at least every 100ms
        influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(T dataPoint, Context context) throws Exception {
        Point.Builder builder = Point.measurement(measurement)
                .time(dataPoint.getTimeStampMs(), TimeUnit.MILLISECONDS)
                .addField(fieldName, dataPoint.getValue());

        if (dataPoint instanceof KeyedDataPoint) {
            builder.tag("key", ((KeyedDataPoint) dataPoint).getKey());
        }

        Point point = builder.build();

        influxDB.write(databaseName, "autogen", point);
    }
}
