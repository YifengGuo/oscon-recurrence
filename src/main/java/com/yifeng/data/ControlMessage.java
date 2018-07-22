package com.yifeng.data;

/**
 * Created by guoyifeng on 7/22/18
 */
public class ControlMessage {
    private String key;
    private double amplitude;

    public ControlMessage(String key, double amplitude) {
        this.key = key;
        this.amplitude = amplitude;
    }

    public String getKey() {
        return key;
    }

    public double getAmplitude() {
        return amplitude;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setAmplitude(double amplitude) {
        this.amplitude = amplitude;
    }

    /**
     * generate ControlMessage object from input string
     * @param s
     * @return
     */
    public static ControlMessage fromString(String s) {
        String[] tokens = s.split(" ");
        if (tokens.length == 2) {
            ControlMessage msg = new ControlMessage(tokens[0], Double.parseDouble(tokens[1]));
            return msg;
        } else {
            return null;
        }
    }
}
