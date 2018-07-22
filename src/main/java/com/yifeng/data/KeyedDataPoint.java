package com.yifeng.data;

/**
 * Created by guoyifeng on 7/22/18
 */
// must extends from DataPoint<T> instead DataPoint
// otherwise the method has same name with super class will have exceptions if not declare generic
public class KeyedDataPoint<T> extends DataPoint<T> {
    private long timeStampMs;
    private String key;
    private T value;

    public KeyedDataPoint() {
        super();
        this.key = null;
    }

    public KeyedDataPoint(long timeStampMs, String key, T value) {
        super(timeStampMs, value);
        this.key = key;
    }

    @Override
    public String toString() {
        return getTimeStampMs()  + "," + getKey() + "," + getValue();
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    /**
     * to modify KeyedDataPoint object's value with potential another type data
     * @param newValue
     * @param <R> newValue's type
     * @return
     */
    public <R> KeyedDataPoint<R> withNewValue(R newValue) {
        return new KeyedDataPoint<R>(this.getTimeStampMs(), this.getKey(), newValue);
    }
}
