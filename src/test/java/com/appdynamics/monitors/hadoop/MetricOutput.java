/*
 * Copyright 2013. AppDynamics LLC and its affiliates.
 *  * All Rights Reserved.
 *  * This is unpublished proprietary source code of AppDynamics LLC and its affiliates.
 *  * The copyright notice above does not evidence any actual or intended publication of such source code.
 */

package com.appdynamics.monitors.hadoop;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by abey.tom on 12/1/16.
 */
public class MetricOutput {
    public static final Logger logger = LoggerFactory.getLogger(MetricOutput.class);

    private String metricPath;
    private BigDecimal value;
    private String metricType;

    public MetricOutput(String metricPath, BigDecimal value, String metricType) {
        this.metricPath = metricPath;
        this.value = value;
        this.metricType = metricType;
    }

    public String getMetricPath() {
        return metricPath;
    }

    public BigDecimal getValue() {
        return value;
    }

    public String getMetricType() {
        return metricType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetricOutput)) return false;

        MetricOutput that = (MetricOutput) o;

        if (!metricPath.equals(that.metricPath)) return false;
        if (!value.equals(that.value)) return false;
        return metricType.equals(that.metricType);
    }

    @Override
    public int hashCode() {
        int result = metricPath.hashCode();
        result = 31 * result + value.hashCode();
        result = 31 * result + metricType.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MetricOutput{" +
                "metricPath='" + metricPath + '\'' +
                ", value=" + value +
                ", metricType='" + metricType + '\'' +
                '}';
    }

    public static List<MetricOutput> from(String classpath) throws IOException {
        List<String> lines = IOUtils.readLines(MetricOutput.class.getResourceAsStream(classpath));
        if (lines != null) {
            List<MetricOutput> expected = new ArrayList<MetricOutput>();
            for (String line : lines) {
                String[] split = line.split(",");
                if (split.length == 3) {
                    MetricOutput metricOutput = new MetricOutput(split[0], new BigDecimal(split[1].trim()), split[2]);
                    expected.add(metricOutput);
                } else {
                    logger.error("Invalid line " + line + " in file " + classpath);
                }
            }
            return expected;
        } else {
            throw new RuntimeException("The resource [" + classpath + "] doesn't exit");
        }
    }
}
