package com.appdynamics.monitors.hadoop.input;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "metricStats")
@XmlAccessorType(XmlAccessType.FIELD)
public class MetricStats {

    @XmlElement(name = "metric-config")
    private MetricConfig[] MetricConfig;

    public MetricConfig[] getMetricConfig() {
        return MetricConfig;
    }

    public void setMetricConfig(MetricConfig[] metricConfig) {
        MetricConfig = metricConfig;
    }
}
