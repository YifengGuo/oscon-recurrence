package com.yifeng.functions;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by guoyifeng on 7/23/18
 */
public class TestSawtoothFunction {
    private static int currentStep;
    private static int counter;
    private static double getData(int numSteps) {
        double phase = currentStep * 1.0 / numSteps;
        currentStep = ++currentStep % numSteps;
        counter++;
        return phase;
    }
    public static void main(String[] args) {
        for (int i = 0; i < 12; i++) {
            System.out.print(getData(10) + " ");
        }
    }
}
