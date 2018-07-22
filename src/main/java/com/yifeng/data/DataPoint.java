package com.yifeng.data;

/**
 * Created by guoyifeng on 7/22/18
 */
public class DataPoint<T> {
    private long timeStampMs;
    private T value;

    // default constructor
    public DataPoint() {
        this.timeStampMs = 0;
        this.value = null;
    }

    public DataPoint(long timeStampMs, T value) {
        this.timeStampMs = timeStampMs;
        this.value = value;
    }

    // getters and setters
    public long getTimeStampMs() {
        return timeStampMs;
    }

    public T getValue() {
        return value;
    }

    public void setTimeStampMs(long timeStampMs) {
        this.timeStampMs = timeStampMs;
    }

    public void setValue(T value) {
        this.value = value;
    }

    /**
     * to modify DataPoint object's value with potential another type data
     * @param newValue
     * @param <R>
     * @return
     */
    public <R> DataPoint<R> withNewValue(R newValue) {
        return new DataPoint<R>(this.getTimeStampMs(), newValue);
    }

    public <R> KeyedDataPoint<R> withNewKeyAndValue(String key, R newValue) {
        return new KeyedDataPoint<R>(this.getTimeStampMs(), key, newValue);
    }

    /**
     * modify DataPoint object's key which can only be a String
     * @param newKey
     * @return
     */
    public KeyedDataPoint withNewKey(String newKey) {
        return new KeyedDataPoint(this.getTimeStampMs(), newKey, this.getValue());
    }
}
