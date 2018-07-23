package com.yifeng.sources;

import com.yifeng.data.DataPoint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Collections;
import java.util.List;

/**
 * Created by guoyifeng on 7/22/18
 */

/**
 * stream source for this demo project
 */
public class TimestampSource extends RichSourceFunction<DataPoint<Long>> implements ListCheckpointed<Long> {
    private final int periodMs;
    private final int slowdownFactor;
    private volatile boolean running = true;

    // Checkpointed State
    private volatile long currentTimeMs = 0;

    public TimestampSource(int periodMs, int slowdownFactor) {
        this.periodMs = periodMs;
        this.slowdownFactor = slowdownFactor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        long now = System.currentTimeMillis();
        if (currentTimeMs == 0) {
            currentTimeMs = now - (now % 1000);
        }
    }

    public void run(SourceContext<DataPoint<Long>> ctx) throws Exception {
        while (running) {
            synchronized (ctx.getCheckpointLock()) {
                // To assign a timestamp to an element in the source directly, the source must use the
                // collectWithTimestamp(...) method on the SourceContext.
                // To generate watermarks, the source must call the emitWatermark(Watermark) function.
                ctx.collectWithTimestamp(new DataPoint<Long>(currentTimeMs, 0L), currentTimeMs);
                ctx.emitWatermark(new Watermark(currentTimeMs));
                currentTimeMs += periodMs;
            }
            timeSync();
        }
    }

    public void cancel() {
        running = false;
    }


    public List<Long> snapshotState(long checkpointId, long checkpointTimeStamp) throws Exception {
        return Collections.singletonList(currentTimeMs);
    }

    public void restoreState(List<Long> state) throws Exception {
        for (Long s : state) {
            currentTimeMs = s; // reset the timestamp to that moment
        }
    }

    private  void timeSync() throws InterruptedException {
        // Sync up with real time
        long realTimeDateMs = currentTimeMs - System.currentTimeMillis();
        long sleepTime = periodMs + realTimeDateMs + randomJitter();

        if (slowdownFactor != 1) {
            sleepTime = periodMs * slowdownFactor;
        }

        if (sleepTime > 0) {
            Thread.sleep(sleepTime);
        }
    }

    private long randomJitter() {
        double sign = -1.0;
        if (Math.random() > 0.5) {
            sign = 1.0;
        }
        return (long)(Math.random() * periodMs * sign);
    }
}
