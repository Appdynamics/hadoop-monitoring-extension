package com.appdynamics.monitors.hadoop.Utility;

import java.math.BigDecimal;

public class MetricUtils {

    public static BigDecimal multiplyAndRound(String value, BigDecimal multiplier) {
        BigDecimal newValue;
        if (multiplier != null) {
            newValue = new BigDecimal(value).multiply(multiplier);
        } else {
            newValue = new BigDecimal(value);
        }
        return newValue.setScale(0, BigDecimal.ROUND_HALF_UP);
    }
}
