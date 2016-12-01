package com.appdynamics.monitors.hadoop.input;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

/**
 * Created by abey.tom on 9/9/16.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class MetricGroup {
    @XmlAttribute
    private String name;
    @XmlElement(name = "metric")
    private Metric[] metrics;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Metric[] getMetrics() {
        return metrics;
    }

    public void setMetrics(Metric[] metrics) {
        this.metrics = metrics;
    }
}
